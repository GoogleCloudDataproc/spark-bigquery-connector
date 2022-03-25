/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import static com.google.cloud.http.BaseHttpServiceException.UNKNOWN_CODE;
import static com.google.common.base.Throwables.getCausalChain;
import static java.lang.String.format;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BigQueryUtil {
  static final ImmutableSet<String> INTERNAL_ERROR_MESSAGES =
      ImmutableSet.of(
          "HTTP/2 error code: INTERNAL_ERROR",
          "Connection closed with unknown cause",
          "Received unexpected EOS on DATA frame from server");
  private static final String PROJECT_PATTERN = "\\S+";
  private static final String DATASET_PATTERN = "\\w+";
  // Allow all non-whitespace beside ':' and '.'.
  // These confuse the qualified table parsing.
  private static final String TABLE_PATTERN = "[\\S&&[^.:]]+";
  /**
   * Regex for an optionally fully qualified table.
   *
   * <p>Must match 'project.dataset.table' OR the legacy 'project:dataset.table' OR 'dataset.table'
   * OR 'table'.
   */
  private static final Pattern QUALIFIED_TABLE_REGEX =
      Pattern.compile(
          format("^(((%s)[:.])?(%s)\\.)?(%s)$$", PROJECT_PATTERN, DATASET_PATTERN, TABLE_PATTERN));

  private BigQueryUtil() {}

  public static boolean isRetryable(Throwable cause) {
    return getCausalChain(cause).stream().anyMatch(BigQueryUtil::isRetryableInternalError);
  }

  static boolean isRetryableInternalError(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      StatusRuntimeException statusRuntimeException = (StatusRuntimeException) t;
      return statusRuntimeException.getStatus().getCode() == Status.Code.INTERNAL
          && INTERNAL_ERROR_MESSAGES.stream()
              .anyMatch(message -> statusRuntimeException.getMessage().contains(message));
    }
    return false;
  }

  static BigQueryException convertToBigQueryException(BigQueryError error) {
    return new BigQueryException(UNKNOWN_CODE, error.getMessage(), error);
  }

  static boolean areCredentialsEqual(Credentials credentials1, Credentials credentials2) {
    if (!(credentials1 instanceof ExternalAccountCredentials)
        && !(credentials2 instanceof ExternalAccountCredentials)) {
      return Objects.equal(credentials1, credentials2);
    }

    return Arrays.equals(
        getCredentialsByteArray(credentials1), getCredentialsByteArray(credentials2));
  }

  static byte[] getCredentialsByteArray(Credentials credentials) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    try {
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(credentials);
      objectOutputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return byteArrayOutputStream.toByteArray();
  }

  // returns the first present optional, empty if all parameters are empty
  public static <T> Optional<T> firstPresent(Optional<T>... optionals) {
    for (Optional<T> o : optionals) {
      if (o.isPresent()) {
        return o;
      }
    }
    return Optional.empty();
  }

  public static TableId parseTableId(String rawTable) {
    return parseTableId(rawTable, Optional.empty(), Optional.empty(), Optional.empty());
  }

  public static TableId parseTableId(
      String rawTable, Optional<String> dataset, Optional<String> project) {
    return parseTableId(rawTable, dataset, project, Optional.empty());
  }

  public static TableId parseTableId(
      String rawTable,
      Optional<String> dataset,
      Optional<String> project,
      Optional<String> datePartition) {
    Matcher matcher = QUALIFIED_TABLE_REGEX.matcher(rawTable);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          format("Invalid Table ID '%s'. Must match '%s'", rawTable, QUALIFIED_TABLE_REGEX));
    }
    String table = matcher.group(5);
    Optional<String> parsedDataset = Optional.ofNullable(matcher.group(4));
    Optional<String> parsedProject = Optional.ofNullable(matcher.group(3));
    String tableAndPartition =
        datePartition.map(date -> String.format("%s$%s", table, date)).orElse(table);
    String actualDataset =
        firstPresent(parsedDataset, dataset)
            .orElseThrow(() -> new IllegalArgumentException("'dataset' not parsed or provided."));
    return firstPresent(parsedProject, project)
        .map(p -> TableId.of(p, actualDataset, tableAndPartition))
        .orElse(TableId.of(actualDataset, tableAndPartition));
  }

  public static String friendlyTableName(TableId tableId) {
    return tableId.getProject() != null
        ? String.format("%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable())
        : String.format("%s.%s", tableId.getDataset(), tableId.getTable());
  }

  public static void convertAndThrow(BigQueryError error) {
    throw new BigQueryException(UNKNOWN_CODE, error.getMessage(), error);
  }

  /**
   * Solving Issue #248. As the bigquery load API can handle wildcard characters, we replace (a) 10
   * URIs ending in XXX[0-9].suffix with XXX*.suffix; (b) 10 URIs ending in XXX[0-9]*.suffix with
   * XXX*.suffix; This reduces the total number of URIs sent to the BigQuery API
   *
   * @param uris the list of URIs where the data was written to
   * @param prefixRegex the regex to catch the part before the file index.
   * @param suffixRegex the regex to catch the part after the file index.
   */
  public static List<String> optimizeLoadUriList(
      List<String> uris, String prefixRegex, String suffixRegex) {
    Pattern pattern = Pattern.compile("(" + prefixRegex + "\\d*)\\d\\**(" + suffixRegex + ")");
    ImmutableList.Builder<String> result = ImmutableList.builder();
    List<String> workingList = uris;
    while (!workingList.isEmpty()) {
      Multimap<String, String> trimmedUriMap = trimUris(pattern, workingList);
      List<String> nextList = new ArrayList<>();
      for (String trimmedUri : trimmedUriMap.keySet()) {
        Collection<String> mappedUris = trimmedUriMap.get(trimmedUri);
        if (mappedUris.size() == 10) {
          // All URIs exists, we can replace the last character with '*' in the next iteration
          nextList.add(trimmedUri);
        } else {
          // not all suffixes exist, so we can't optimize further.
          result.addAll(mappedUris);
        }
      }
      // cleanup and prepare next iteration
      trimmedUriMap.clear();
      workingList = nextList;
    }
    return result.build();
  }

  private static Multimap<String, String> trimUris(Pattern pattern, List<String> workingList) {
    Multimap<String, String> trimmedUriMap = HashMultimap.create();
    // converting
    // prefix-1234.suffix to prefix-123*.suffix
    // prefix-123*.suffix to prefix-12*.suffix
    for (String uri : workingList) {
      String trimmedUri = pattern.matcher(uri).replaceFirst("$1*$2");
      trimmedUriMap.put(trimmedUri, uri);
    }
    return trimmedUriMap;
  }

  /**
   * Compares to Schema instances for equality. Unlike Schema.equals(), here the caller can
   * disregard the schema's field order.
   *
   * @param s1 the first schema to compare
   * @param s2 the second schema to compare
   * @param regardFieldOrder whether to regard the field order in the comparison
   * @return true is the two schema equal each other, false otherwise
   */
  public static boolean schemaEquals(Schema s1, Schema s2, boolean regardFieldOrder) {
    if (s1 == s2) {
      return true;
    }
    // if both are null we would have caught it earlier
    if (s1 == null || s2 == null) {
      return false;
    }
    if (regardFieldOrder) {
      return s1.equals(s2);
    }
    // compare field by field
    return fieldListEquals(s1.getFields(), s2.getFields());
  }

  // We need this method as the BigQuery API may leave the mode field as null in case of NULLABLE
  @VisibleForTesting
  static boolean fieldEquals(Field f1, Field f2) {
    if (f1 == f2) {
      return true;
    }
    // if both are null we would have caught it earlier
    if (f1 == null || f2 == null) {
      return false;
    }

    if (!fieldListEquals(f1.getSubFields(), f2.getSubFields())) {
      return false;
    }

    return Objects.equal(f1.getName(), f2.getName())
        && Objects.equal(f1.getType(), f2.getType())
        && Objects.equal(nullableIfNull(f1.getMode()), nullableIfNull(f2.getMode()))
        && Objects.equal(f1.getDescription(), f2.getDescription())
        && Objects.equal(f1.getPolicyTags(), f2.getPolicyTags())
        && Objects.equal(f1.getMaxLength(), f2.getMaxLength())
        && Objects.equal(f1.getScale(), f2.getScale())
        && Objects.equal(f1.getPrecision(), f2.getPrecision());
  }

  @VisibleForTesting
  static boolean fieldListEquals(FieldList fl1, FieldList fl2) {
    if (fl1 == fl2) {
      return true;
    }
    // if both are null we would have caught it earlier
    if (fl1 == null || fl2 == null) {
      return false;
    }

    Map<String, Field> fieldsMap1 =
        fl1.stream().collect(Collectors.toMap(Field::getName, Function.identity()));
    Map<String, Field> fieldsMap2 =
        fl2.stream().collect(Collectors.toMap(Field::getName, Function.identity()));

    for (Map.Entry<String, Field> e : fieldsMap1.entrySet()) {
      Field f1 = e.getValue();
      Field f2 = fieldsMap2.get(e.getKey());
      if (!fieldEquals(f1, f2)) {
        return false;
      }
    }

    return true;
  }

  static Field.Mode nullableIfNull(Field.Mode mode) {
    return mode == null ? Field.Mode.NULLABLE : mode;
  }

  public static Optional<String> emptyIfNeeded(String value) {
    return (value == null || value.length() == 0) ? Optional.empty() : Optional.of(value);
  }
}
