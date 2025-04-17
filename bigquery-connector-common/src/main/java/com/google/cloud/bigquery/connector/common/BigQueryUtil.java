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

import static com.google.cloud.bigquery.Field.Mode.NULLABLE;
import static com.google.cloud.http.BaseHttpServiceException.UNKNOWN_CODE;
import static com.google.common.base.Throwables.getCausalChain;
import static java.lang.String.format;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.HivePartitioningOptions;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class BigQueryUtil {

  // Numeric is a fixed precision Decimal Type with 38 digits of precision and 9 digits of scale.
  // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
  public static final int DEFAULT_NUMERIC_PRECISION = 38;
  public static final int DEFAULT_NUMERIC_SCALE = 9;
  public static final int DEFAULT_BIG_NUMERIC_PRECISION = 76;
  public static final int DEFAULT_BIG_NUMERIC_SCALE = 38;
  private static final int NO_VALUE = -1;
  private static final long BIGQUERY_INTEGER_MIN_VALUE = Long.MIN_VALUE;
  static final ImmutableSet<String> INTERNAL_ERROR_MESSAGES =
      ImmutableSet.of(
          "HTTP/2 error code: INTERNAL_ERROR",
          "Connection closed with unknown cause",
          "Received unexpected EOS on DATA frame from server");

  static final String READ_SESSION_EXPIRED_ERROR_MESSAGE = "session expired at";

  private static final String PROJECT_PATTERN = "\\S+";
  private static final String DATASET_PATTERN = "\\w+";
  // Allow all non-whitespace beside ':' and '.'.
  // These confuse the qualified table parsing.
  private static final String TABLE_PATTERN = "[\\S&&[^.:]]+";

  private static final String NAMED_PARAM_PREFIX = "namedparameters.";
  private static final String POSITIONAL_PARAM_PREFIX = "positionalparameters.";

  /**
   * Regex for an optionally fully qualified table.
   *
   * <p>Must match 'project.dataset.table' OR the legacy 'project:dataset.table' OR 'dataset.table'
   * OR 'table'.
   */
  private static final Pattern QUALIFIED_TABLE_REGEX =
      Pattern.compile(
          format("^(((%s)[:.])?(%s)\\.)?(%s)$$", PROJECT_PATTERN, DATASET_PATTERN, TABLE_PATTERN));

  // Based on
  // https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
  private static final int MAX_FILTER_LENGTH_IN_BYTES = 2 << 20;

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

  public static boolean isReadSessionExpired(Throwable cause) {
    return getCausalChain(cause).stream().anyMatch(BigQueryUtil::isReadSessionExpiredInternalError);
  }

  static boolean isReadSessionExpiredInternalError(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      StatusRuntimeException statusRuntimeException = (StatusRuntimeException) t;
      return statusRuntimeException.getStatus().getCode() == Code.FAILED_PRECONDITION
          && statusRuntimeException.getMessage().contains(READ_SESSION_EXPIRED_ERROR_MESSAGE);
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

  static Credentials getCredentialsFromByteArray(byte[] byteArray) {
    try {
      ObjectInputStream objectInputStream =
          new ObjectInputStream(new ByteArrayInputStream(byteArray));
      return (Credentials) objectInputStream.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
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
   * @param sourceSchema the first schema to compare
   * @param destinationSchema the second schema to compare
   * @param regardFieldOrder whether to regard the field order in the comparison
   * @return {@link ComparisonResult#equal()} is the two schema equal each other, {@link
   *     ComparisonResult#differentWithDescription(List)} otherwise
   */
  public static ComparisonResult schemaWritable(
      Schema sourceSchema,
      Schema destinationSchema,
      boolean regardFieldOrder,
      boolean enableModeCheckForSchemaFields) {
    if (sourceSchema == destinationSchema) {
      return ComparisonResult.equal();
    }
    // if both are null we would have caught it earlier
    if (sourceSchema == null || destinationSchema == null) {
      return ComparisonResult.differentNoDescription();
    }
    if (regardFieldOrder) {
      boolean equalsResult = sourceSchema.equals(destinationSchema);
      return ComparisonResult.fromEqualsResult(equalsResult);
    }
    // compare field by field
    return fieldListWritable(
        sourceSchema.getFields(), destinationSchema.getFields(), enableModeCheckForSchemaFields);
  }

  /**
   * Create a list of read stream names of given read session.
   *
   * @param readSession BQ read session handle
   * @return list of stream names, empty if read session is null
   */
  public static List<String> getStreamNames(ReadSession readSession) {
    return readSession == null
        ? new ArrayList<>()
        : readSession.getStreamsList().stream()
            .map(ReadStream::getName)
            // This formulation is used to guarantee a serializable list.
            .collect(Collectors.toCollection(ArrayList::new));
  }

  /**
   * Check to see if the source field can be writen into the destination field. We need this method
   * as the BigQuery API may leave the mode field as null in case of NULLABLE
   *
   * @param sourceField the field which is to be written
   * @param destinationField the field into which the source is to be written
   * @param enableModeCheckForSchemaFields if true, both the fields' mode checks are compared,
   *     skipped otherwise
   * @return {@link ComparisonResult#equal()} if the source field is writable into destination
   *     field, {@link ComparisonResult#differentWithDescription(List)} otherwise
   */
  @VisibleForTesting
  static ComparisonResult fieldWritable(
      Field sourceField, Field destinationField, boolean enableModeCheckForSchemaFields) {
    if (sourceField == destinationField) {
      return ComparisonResult.equal();
    }

    /* If the destination field is NULLABLE or REPEATED and there is no matching field in the source then it is supported
    but if the destination field is REQUIRED then we do need source field to write into it. */
    if (sourceField == null) {
      if (destinationField.getMode() == Mode.REQUIRED) {
        return ComparisonResult.differentWithDescription(
            ImmutableList.of("Required field not found in source: " + destinationField.getName()));
      } else {
        return ComparisonResult.equal();
      }
    }

    // cannot write if the destination table doesn't have the field
    if (destinationField == null) {
      return ComparisonResult.differentWithDescription(
          ImmutableList.of("Field not found in destination: " + sourceField.getName()));
    }

    if (!Objects.equal(sourceField.getName(), destinationField.getName())) {
      return ComparisonResult.differentWithDescription(
          ImmutableList.of(
              "Wrong field name",
              "expected source field name: "
                  + destinationField.getName()
                  + " but was: "
                  + sourceField.getName()));
    }

    ComparisonResult subFieldsResult =
        fieldListWritable(
            sourceField.getSubFields(),
            destinationField.getSubFields(),
            enableModeCheckForSchemaFields);
    if (!subFieldsResult.valuesAreEqual()) {
      return ComparisonResult.differentWithDescription(
          ImmutableList.of(
              "Subfields mismatch for: " + destinationField.getName(),
              subFieldsResult.makeMessage()));
    }

    if (!typeWriteable(sourceField.getType(), destinationField.getType())) {
      return ComparisonResult.differentWithDescription(
          ImmutableList.of(
              "Incompatible type for field: " + destinationField.getName(),
              "cannot write source type: "
                  + sourceField.getType()
                  + " to destination type: "
                  + destinationField.getType()));
    }
    if (enableModeCheckForSchemaFields
        && !isModeWritable(
            nullableIfNull(sourceField.getMode()), nullableIfNull(destinationField.getMode()))) {
      return ComparisonResult.differentWithDescription(
          ImmutableList.of(
              "Incompatible mode for field: " + destinationField.getName(),
              "cannot write source field mode: "
                  + nullableIfNull(sourceField.getMode()).name()
                  + " to destination field mode: "
                  + nullableIfNull(destinationField.getMode()).name()));
    }

    boolean sourceAndDestMaxLengthNull =
        (sourceField.getMaxLength() == null && destinationField.getMaxLength() == null);
    boolean sourceMaxLenLessThanEqualsDestMaxLen =
        (sourceField.getMaxLength() != null
            && destinationField.getMaxLength() != null
            && sourceField.getMaxLength() <= destinationField.getMaxLength());
    if ((!sourceAndDestMaxLengthNull) && (!sourceMaxLenLessThanEqualsDestMaxLen)) {
      return ComparisonResult.differentWithDescription(
          ImmutableList.of(
              "Incompatible max length for field: " + destinationField.getName(),
              "cannot write source field with max length: "
                  + sourceField.getMaxLength()
                  + " to destination field with max length: "
                  + destinationField.getMaxLength()));
    }

    boolean sourceScaleLessThanEqualsDestScale =
        ((sourceField.getScale() == destinationField.getScale())
            || (getScale(sourceField) <= getScale(destinationField)));
    boolean sourcePrecisionLessThanEqualsDestPrecision =
        ((sourceField.getPrecision() == destinationField.getPrecision())
            || (getPrecision(sourceField) <= getPrecision(destinationField)));
    if ((!sourceScaleLessThanEqualsDestScale) || (!sourcePrecisionLessThanEqualsDestPrecision)) {
      return ComparisonResult.differentWithDescription(
          ImmutableList.of(
              "Incompatible precision, scale for field: " + destinationField.getName(),
              "cannot write source field with precision, scale: "
                  + formatPrecisionAndScale(sourceField)
                  + " to destination field with precision, scale: "
                  + formatPrecisionAndScale(destinationField)));
    }

    return ComparisonResult.equal();
  }

  private static String formatPrecisionAndScale(Field field) {
    return String.format("(%s, %s)", field.getPrecision(), field.getScale());
  }

  // allowing widening narrow numeric into bignumeric
  // allowing writing string to time
  @VisibleForTesting
  static boolean typeWriteable(LegacySQLTypeName sourceType, LegacySQLTypeName destinationType) {
    return (sourceType.equals(LegacySQLTypeName.NUMERIC)
            && destinationType.equals(LegacySQLTypeName.BIGNUMERIC))
        || (sourceType.equals(LegacySQLTypeName.STRING)
            && destinationType.equals(LegacySQLTypeName.TIME))
        || (sourceType.equals(LegacySQLTypeName.STRING)
            && destinationType.equals(LegacySQLTypeName.DATETIME))
        || sourceType.equals(destinationType);
  }

  @VisibleForTesting
  static int getPrecision(Field field) {
    return getValueOrDefault(
        field.getPrecision(),
        field.getType(),
        DEFAULT_NUMERIC_PRECISION,
        DEFAULT_BIG_NUMERIC_PRECISION);
  }

  @VisibleForTesting
  static int getScale(Field field) {
    return getValueOrDefault(
        field.getScale(), field.getType(), DEFAULT_NUMERIC_SCALE, DEFAULT_BIG_NUMERIC_SCALE);
  }

  private static int getValueOrDefault(
      Long value, LegacySQLTypeName type, int numericValue, int bigNumericValue) {
    if (value != null) {
      return value.intValue();
    }
    // scale is null, so use defaults
    if (LegacySQLTypeName.NUMERIC.equals(type)) {
      return numericValue;
    }
    if (LegacySQLTypeName.BIGNUMERIC.equals(type)) {
      return bigNumericValue;
    }
    return NO_VALUE;
  }

  @VisibleForTesting
  static boolean isModeWritable(Field.Mode sourceMode, Field.Mode destinationMode) {
    switch (destinationMode) {
      case REPEATED:
        return sourceMode == Mode.REPEATED;
      case REQUIRED:
      case NULLABLE:
        return sourceMode != Mode.REPEATED;
    }
    return false;
  }

  @VisibleForTesting
  static ComparisonResult fieldListWritable(
      FieldList sourceFieldList,
      FieldList destinationFieldList,
      boolean enableModeCheckForSchemaFields) {
    if (sourceFieldList == destinationFieldList) {
      return ComparisonResult.equal();
    }

    // if both are null we would have caught it earlier
    if (sourceFieldList == null || destinationFieldList == null) {
      return ComparisonResult.differentNoDescription();
    }

    // cannot write of the source has more fields than the destination table.
    if (sourceFieldList.size() > destinationFieldList.size()) {
      return ComparisonResult.differentWithDescription(
          ImmutableList.of(
              "Number of source fields: "
                  + sourceFieldList.size()
                  + " is larger than number of destination fields: "
                  + destinationFieldList.size()));
    }

    Map<String, Field> sourceFieldsMap =
        sourceFieldList.stream().collect(Collectors.toMap(Field::getName, Function.identity()));
    Map<String, Field> destinationFieldsMap =
        destinationFieldList.stream()
            .collect(Collectors.toMap(Field::getName, Function.identity()));

    for (Map.Entry<String, Field> e : destinationFieldsMap.entrySet()) {
      Field f1 = sourceFieldsMap.get(e.getKey());
      Field f2 = e.getValue();
      ComparisonResult result = fieldWritable(f1, f2, enableModeCheckForSchemaFields);
      if (!result.valuesAreEqual()) {
        return result;
      }
    }

    return ComparisonResult.equal();
  }

  static Field.Mode nullableIfNull(Field.Mode mode) {
    return mode == null ? NULLABLE : mode;
  }

  public static Optional<String> emptyIfNeeded(String value) {
    return (value == null || value.length() == 0) ? Optional.empty() : Optional.of(value);
  }

  /**
   * Create an instance of the given class name, and verify that it is an instance of the required
   * class
   */
  public static <T> T createVerifiedInstance(
      String fullyQualifiedClassName, Class<T> requiredClass, Object... constructorArgs) {
    try {
      Class<?> clazz = Class.forName(fullyQualifiedClassName);
      Object result =
          clazz
              .getDeclaredConstructor(
                  Arrays.stream(constructorArgs)
                      .map(Object::getClass)
                      .toArray((IntFunction<Class<?>[]>) Class[]::new))
              .newInstance(constructorArgs);
      if (!requiredClass.isInstance(result)) {
        throw new IllegalArgumentException(
            String.format(
                "% does not implement %s",
                clazz.getCanonicalName(), requiredClass.getCanonicalName()));
      }
      return (T) result;
    } catch (ClassNotFoundException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Could not instantiate class [%s], implementing %s",
              fullyQualifiedClassName, requiredClass.getCanonicalName()),
          e);
    }
  }

  /**
   * Verify the given object is Serializable by returning the deserialized version of the serialized
   * instance
   */
  public static <T> T verifySerialization(T obj) {
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(buffer);
      out.writeObject(obj);
      out.flush();
      out.close();
      ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray()));
      T result = (T) in.readObject();
      in.close();
      return result;
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Serialization test of " + obj.getClass().getCanonicalName() + " failed", e);
    }
  }

  public static ImmutableList<String> getPartitionFields(TableInfo tableInfo) {
    TableDefinition definition = tableInfo.getDefinition();
    if (definition instanceof StandardTableDefinition) {
      @SuppressWarnings("Varifier")
      StandardTableDefinition sdt = (StandardTableDefinition) definition;
      TimePartitioning timePartitioning = sdt.getTimePartitioning();
      if (timePartitioning != null) {
        if (timePartitioning.getField() == null) {
          // table contains pseudo columns
          List<String> timePartitionFields = new ArrayList<>();
          timePartitionFields.add("_PARTITIONTIME");
          if (timePartitioning.getType().equals(TimePartitioning.Type.DAY)) {
            timePartitionFields.add("_PARTITIONDATE");
          }
          return ImmutableList.copyOf(timePartitionFields);
        }
        return ImmutableList.of(timePartitioning.getField());
      }
      RangePartitioning rangePartitioning = sdt.getRangePartitioning();
      if (rangePartitioning != null) {
        return ImmutableList.of(rangePartitioning.getField());
      }
    }
    if (definition instanceof ExternalTableDefinition) {
      @SuppressWarnings("Varifier")
      ExternalTableDefinition edt = (ExternalTableDefinition) definition;
      HivePartitioningOptions hivePartitioningOptions = edt.getHivePartitioningOptions();
      if (hivePartitioningOptions != null) {
        List<String> fields = hivePartitioningOptions.getFields();
        if (fields != null && !fields.isEmpty()) {
          return ImmutableList.copyOf(fields);
        }
      }
    }
    // no partitioning
    return ImmutableList.of();
  }

  public static ImmutableList<String> getClusteringFields(TableInfo tableInfo) {
    TableDefinition definition = tableInfo.getDefinition();
    if (!(definition instanceof StandardTableDefinition)) {
      return ImmutableList.of();
    }

    Clustering clustering = ((StandardTableDefinition) definition).getClustering();
    if (clustering == null || clustering.getFields() == null) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(clustering.getFields());
  }

  public static boolean filterLengthInLimit(Optional<String> filter) {
    return filter
        .map(f -> f.getBytes(StandardCharsets.UTF_8).length < MAX_FILTER_LENGTH_IN_BYTES)
        .orElse(Boolean.TRUE);
  }

  /**
   * Adjusts the wanted schema to properly match the schema of an existing table.
   *
   * @param wantedSchema
   * @param existingTableSchema
   * @return the adjusted schema
   */
  public static Schema adjustSchemaIfNeeded(
      Schema wantedSchema, Schema existingTableSchema, boolean allowFieldRelaxation) {
    FieldList fields = wantedSchema.getFields();
    FieldList existingFields = existingTableSchema.getFields();
    Map<String, Field> existingFieldsMap =
        existingFields.stream().collect(Collectors.toMap(Field::getName, Function.identity()));
    List<Field> adjustedFields =
        fields.stream()
            .map(
                field ->
                    adjustField(
                        field, existingFieldsMap.get(field.getName()), allowFieldRelaxation))
            .collect(Collectors.toList());
    return Schema.of(adjustedFields);
  }

  /**
   * At the moment converts numeric fields to bignumeric if the exisitng schema requires it. Can be
   * used for other adjustments
   *
   * @param field
   * @param existingField
   * @return the adjusted field
   */
  @VisibleForTesting
  static Field adjustField(
      Field field, @Nullable Field existingField, boolean allowFieldRelaxation) {
    if (existingField == null) {
      return field;
    }
    Mode fieldMode = existingField.getMode() != null ? existingField.getMode() : NULLABLE;
    if (allowFieldRelaxation && field.getMode() == NULLABLE) {
      fieldMode = NULLABLE;
    }

    if (field.getType().equals(LegacySQLTypeName.RECORD)
        && existingField.getType().equals(LegacySQLTypeName.RECORD)) {
      // need to go recursively
      FieldList subFields = field.getSubFields();
      FieldList exitingSubFields = existingField.getSubFields();
      Map<String, Field> existingSubFieldsMap =
          exitingSubFields.stream().collect(Collectors.toMap(Field::getName, Function.identity()));
      FieldList adjustedSubFields =
          FieldList.of(
              subFields.stream()
                  .map(
                      subField ->
                          adjustField(
                              subField,
                              existingSubFieldsMap.get(subField.getName()),
                              allowFieldRelaxation))
                  .collect(Collectors.toList()));
      return existingField.toBuilder().setType(LegacySQLTypeName.RECORD, adjustedSubFields).build();
    }
    // no adjustment
    return existingField.toBuilder().setMode(fieldMode).build();
  }

  public static String prepareQueryForLog(String query, int maxLength) {
    String noNewLinesQuery = query.replace("\n", "\\n");
    return noNewLinesQuery.length() > maxLength
        ? noNewLinesQuery.substring(0, maxLength) + /* ellipsis */ '\u2026'
        : noNewLinesQuery;
  }

  static String getQueryForTimePartitionedTable(
      String destinationTableName,
      String temporaryTableName,
      StandardTableDefinition destinationDefinition,
      TimePartitioning timePartitioning) {
    TimePartitioning.Type partitionType = timePartitioning.getType();
    String partitionField = timePartitioning.getField();
    FieldList allFields = destinationDefinition.getSchema().getFields();
    Optional<LegacySQLTypeName> partitionFieldType =
        allFields.stream()
            .filter(field -> partitionField.equals(field.getName()))
            .map(field -> field.getType())
            .findFirst();
    // Using timestamp_trunc on a DATE field results in $cast_to_DATETIME which prevents pruning
    // when required_partition_filter is set, as it is not considered a monotonic function
    String truncFuntion =
        (partitionFieldType.isPresent() && partitionFieldType.get().equals(LegacySQLTypeName.DATE))
            ? "date_trunc"
            : "timestamp_trunc";
    String extractedPartitionedSource =
        String.format("%s(`%s`, %s)", truncFuntion, partitionField, partitionType.toString());
    String extractedPartitionedTarget =
        String.format(
            "%s(`target`.`%s`, %s)", truncFuntion, partitionField, partitionType.toString());

    String commaSeparatedFields =
        allFields.stream().map(Field::getName).collect(Collectors.joining("`,`", "`", "`"));

    String queryFormat =
        "DECLARE partitions_to_delete DEFAULT "
            + "(SELECT ARRAY_AGG(DISTINCT(%s) IGNORE NULLS) FROM `%s`); \n"
            + "MERGE `%s` AS target\n"
            + "USING `%s` AS source\n"
            + "ON FALSE\n"
            + "WHEN NOT MATCHED BY SOURCE AND %s IN UNNEST(partitions_to_delete) THEN DELETE\n"
            + "WHEN NOT MATCHED BY TARGET THEN\n"
            + "INSERT(%s) VALUES(%s)";
    return String.format(
        queryFormat,
        extractedPartitionedSource,
        temporaryTableName,
        destinationTableName,
        temporaryTableName,
        extractedPartitionedTarget,
        commaSeparatedFields,
        commaSeparatedFields);
  }

  static String getQueryForRangePartitionedTable(
      String destinationTableName,
      String temporaryTableName,
      StandardTableDefinition destinationDefinition,
      RangePartitioning rangePartitioning) {

    long start = rangePartitioning.getRange().getStart();
    long end = rangePartitioning.getRange().getEnd();
    long interval = rangePartitioning.getRange().getInterval();

    String partitionField = rangePartitioning.getField();
    String extractedPartitioned =
        "IFNULL(IF(%s.%s >= %s, 0, RANGE_BUCKET(%s.%s, GENERATE_ARRAY(%s, %s, %s))), -1)";
    String extractedPartitionedSource =
        String.format(
            extractedPartitioned,
            "source",
            partitionField,
            end,
            "source",
            partitionField,
            start,
            end,
            interval);
    String extractedPartitionedTarget =
        String.format(
            extractedPartitioned,
            "target",
            partitionField,
            end,
            "target",
            partitionField,
            start,
            end,
            interval);

    FieldList allFields = destinationDefinition.getSchema().getFields();
    String commaSeparatedFields =
        allFields.stream().map(Field::getName).collect(Collectors.joining("`,`", "`", "`"));
    String booleanInjectedColumn = "_" + Long.toString(1234567890123456789L);

    String queryFormat =
        "MERGE `%s` AS target\n"
            + "USING (SELECT * FROM `%s` CROSS JOIN UNNEST([true, false])  %s) AS source\n"
            + "ON %s = %s AND %s AND (target.%s >= %d OR target.%s IS NULL )\n"
            + "WHEN MATCHED THEN DELETE\n"
            + "WHEN NOT MATCHED AND NOT %s THEN\n"
            + "INSERT(%s) VALUES(%s)";
    return String.format(
        queryFormat,
        destinationTableName,
        temporaryTableName,
        booleanInjectedColumn,
        extractedPartitionedSource,
        extractedPartitionedTarget,
        booleanInjectedColumn,
        partitionField,
        BIGQUERY_INTEGER_MIN_VALUE,
        partitionField,
        booleanInjectedColumn,
        commaSeparatedFields,
        commaSeparatedFields);
  }

  // based on https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration, it
  // seems that the values are subject to the same restriction
  public static String sanitizeLabelValue(String value) {
    int resultLength = Math.min(value.length(), 63);
    StringBuilder buf = new StringBuilder(value.length());
    for (int i = 0; i < resultLength; i++) {
      char ch = Character.toLowerCase(value.charAt(i));
      if (Character.isLetterOrDigit(ch) || ch == '-' || ch == '_') {
        buf.append(ch);
      } else {
        // if not alpha-numeric or -,_ then append underscore as a placeholder
        buf.append('_');
      }
    }
    return buf.toString();
  }

  /**
   * BigLake Managed tables are not represented by a dedicated type. Instead, their presence is
   * indicated by the field 'bigLakeConfiguration' within the StandardTableDefinition {@link
   * StandardTableDefinition#getBigLakeConfiguration()}.
   *
   * @param table the table to check
   * @return Returns true if the table is a BigLake Managed table.
   */
  public static boolean isBigLakeManagedTable(TableInfo table) {
    TableDefinition tableDefinition = table.getDefinition();
    return tableDefinition.getType() == TableDefinition.Type.TABLE
        && tableDefinition instanceof StandardTableDefinition
        && ((StandardTableDefinition) tableDefinition).getBigLakeConfiguration() != null;
  }

  /**
   * Since StandardTableDefinition (table_type == TableDefinition.Type.TABLE) can represent both
   * BigQuery native tables and BigLake Managed tables, the absence of the "bigLakeConfiguration"
   * field within the StandardTableDefinition {@link
   * StandardTableDefinition#getBigLakeConfiguration()}. indicates a BigQuery native table.
   *
   * @param table the table to check
   * @return Returns true if the table is a BigQuery Native table.
   */
  public static boolean isBigQueryNativeTable(TableInfo table) {
    return table.getDefinition().getType() == TableDefinition.Type.TABLE
        && !isBigLakeManagedTable(table);
  }

  /**
   * Parses query parameters from Spark options based on NamedParameters.* and
   * PositionalParameters.* prefixes.
   *
   * @param options A map containing Spark DataSource options.
   * @return A QueryParameterHelper object containing either named or positional parameters, or an
   *     empty state (ParameterMode.NONE).
   * @throws IllegalArgumentException if both named and positional parameters are specified, if
   *     parameter format is invalid, type is unknown/unsupported, value parsing fails, positional
   *     index is invalid/missing/duplicated, or positional parameters are not contiguous from 1.
   */
  public static QueryParameterHelper parseQueryParameters(Map<String, String> options) {
    Map<String, QueryParameterValue> namedParameters = new HashMap<>();
    Map<Integer, QueryParameterValue> positionalParametersTemp = new TreeMap<>();
    ParameterMode currentMode = ParameterMode.NONE;

    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      String keyLower = key.toLowerCase(Locale.ROOT);
      String value = entry.getValue();

      if (keyLower.startsWith(NAMED_PARAM_PREFIX)) {
        Preconditions.checkArgument(
            currentMode == ParameterMode.NAMED || currentMode == ParameterMode.NONE,
            "Cannot mix NamedParameters.* and PositionalParameters.* options.");
        currentMode = ParameterMode.NAMED;

        processNamedParameter(key, value, namedParameters);
      } else if (keyLower.startsWith(POSITIONAL_PARAM_PREFIX)) {
        Preconditions.checkArgument(
            currentMode == ParameterMode.POSITIONAL || currentMode == ParameterMode.NONE,
            "Cannot mix NamedParameters.* and PositionalParameters.* options.");
        currentMode = ParameterMode.POSITIONAL;
        processPositionalParameter(key, value, positionalParametersTemp);
      }
    }

    switch (currentMode) {
      case NAMED:
        return QueryParameterHelper.named(namedParameters);
      case POSITIONAL:
        List<QueryParameterValue> positionalList =
            buildPositionalParameterList(positionalParametersTemp);
        return QueryParameterHelper.positional(positionalList);
      case NONE:
      default:
        return QueryParameterHelper.none();
    }
  }

  /**
   * Processes a single named parameter option, adding it to the map. Performs validation checks
   * specific to named parameters.
   */
  private static void processNamedParameter(
      String key, String value, Map<String, QueryParameterValue> namedParams) {

    String paramName = key.substring(NAMED_PARAM_PREFIX.length());
    Preconditions.checkArgument(
        !paramName.isEmpty(),
        String.format("Named parameter name cannot be empty. Option key: '%s'", key));

    QueryParameterValue qpv = parseSingleParameterValue(paramName, value); // Identifier is name

    // Check for duplicates *before* putting
    Preconditions.checkArgument(
        !namedParams.containsKey(paramName),
        String.format(
            "Duplicate named parameter definition for '%s'. Option key: '%s'", paramName, key));

    namedParams.put(paramName, qpv);
  }

  /**
   * Processes a single positional parameter option, adding it to the temporary map. Performs
   * parsing and validation checks specific to positional parameters.
   */
  private static void processPositionalParameter(
      String key, String value, Map<Integer, QueryParameterValue> positionalParamsTemp) {

    String indexStr = key.substring(POSITIONAL_PARAM_PREFIX.length());
    Preconditions.checkArgument(
        !indexStr.isEmpty(),
        String.format("Positional parameter index cannot be empty. Option key: '%s'", key));

    int index;
    try {
      index = Integer.parseInt(indexStr);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid positional parameter index: '%s' must be an integer. Option key: '%s'",
              indexStr, key),
          e);
    }

    Preconditions.checkArgument(
        index >= 1,
        String.format(
            "Invalid positional parameter index: %d. Indices must be 1-based. Option key: '%s'",
            index, key));

    QueryParameterValue qpv =
        parseSingleParameterValue(String.valueOf(index), value); // Identifier is index

    Preconditions.checkArgument(
        !positionalParamsTemp.containsKey(index),
        String.format(
            "Duplicate positional parameter definition for index: %d. Option key: '%s'",
            index, key));

    positionalParamsTemp.put(index, qpv);
  }

  /**
   * Builds the final ordered list of positional parameters from the temporary map, checking for
   * contiguous indices starting from 1.
   *
   * @param positionalParamsTemp Sorted map of index to parameter value.
   * @return Unmodifiable list of positional parameters in order.
   * @throws NullPointerException if a gap is found (via Preconditions.checkNotNull).
   */
  private static List<QueryParameterValue> buildPositionalParameterList(
      Map<Integer, QueryParameterValue> positionalParamsTemp) {
    List<QueryParameterValue> positionalList = new ArrayList<>();
    int maxIndex = ((TreeMap<Integer, QueryParameterValue>) positionalParamsTemp).lastKey();

    for (int i = 1; i <= maxIndex; i++) {
      QueryParameterValue currentParam = positionalParamsTemp.get(i);
      Preconditions.checkNotNull(
          currentParam,
          String.format(
              "Missing positional parameter for index: %d. Parameters must be contiguous starting from 1.",
              i));
      positionalList.add(currentParam);
    }
    return positionalList;
  }

  /**
   * Parses a "TYPE:value" string into a QueryParameterValue.
   *
   * @param identifier The name or index (as string) of the parameter, for error messages.
   * @param typeValueString The combined "TYPE:value" string.
   * @return The corresponding QueryParameterValue.
   * @throws IllegalArgumentException if the format is invalid, type is unknown/unsupported, or
   *     value parsing fails.
   */
  private static QueryParameterValue parseSingleParameterValue(
      String identifier, String typeValueString) {
    Preconditions.checkNotNull(
        typeValueString,
        String.format("Parameter value string cannot be null for identifier: %s", identifier));

    int colonIndex = typeValueString.indexOf(':');
    Preconditions.checkArgument(
        colonIndex > 0,
        String.format(
            "Invalid parameter value format for identifier '%s': '%s'. Expected 'TYPE:value' with"
                + " non-empty TYPE before ':'.",
            identifier, typeValueString));

    String typeStr = typeValueString.substring(0, colonIndex).trim().toUpperCase(Locale.ROOT);
    String valueStr = typeValueString.substring(colonIndex + 1);

    StandardSQLTypeName type;
    try {
      type = StandardSQLTypeName.valueOf(typeStr);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown query parameter type: '%s' for identifier: '%s'. Supported types: %s",
              typeStr,
              identifier,
              Arrays.stream(StandardSQLTypeName.values())
                  .map(Enum::name)
                  .collect(Collectors.joining(", "))),
          e);
    }

    Preconditions.checkArgument(
        type != StandardSQLTypeName.ARRAY && type != StandardSQLTypeName.STRUCT,
        String.format(
            "Unsupported query parameter type: %s for identifier: '%s'. ARRAY and STRUCT types are"
                + " not currently supported as query parameters.",
            type, identifier));

    return QueryParameterValue.newBuilder().setValue(valueStr.trim()).setType(type).build();
  }
}
