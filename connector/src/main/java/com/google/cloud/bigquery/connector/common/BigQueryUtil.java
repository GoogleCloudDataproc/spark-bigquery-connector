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

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.cloud.http.BaseHttpServiceException.UNKNOWN_CODE;
import static com.google.common.base.Throwables.getCausalChain;
import static java.lang.String.format;

public class BigQueryUtil {
    static final ImmutableSet<String> INTERNAL_ERROR_MESSAGES = ImmutableSet.of(
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
     * <p>
     * Must match 'project.dataset.table' OR the legacy 'project:dataset.table' OR 'dataset.table'
     * OR 'table'.
     */
    private static final Pattern QUALIFIED_TABLE_REGEX =
            Pattern.compile(format("^(((%s)[:.])?(%s)\\.)?(%s)$$", PROJECT_PATTERN, DATASET_PATTERN, TABLE_PATTERN));

    private BigQueryUtil() {
    }

    static boolean isRetryable(Throwable cause) {
        return getCausalChain(cause).stream().anyMatch(BigQueryUtil::isRetryableInternalError);
    }

    static boolean isRetryableInternalError(Throwable t) {
        if (t instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException) t;
            return statusRuntimeException.getStatus().getCode() == Status.Code.INTERNAL &&
                    INTERNAL_ERROR_MESSAGES.stream()
                            .anyMatch(message -> statusRuntimeException.getMessage().contains(message));
        }
        return false;
    }

    static BigQueryException convertToBigQueryException(BigQueryError error) {
        return new BigQueryException(UNKNOWN_CODE, error.getMessage(), error);
    }

    // returns the first present optional, empty if all parameters are empty
    public static <T> Optional<T> firstPresent(Optional<T>... optionals) {
        return Stream.of(optionals)
                .flatMap(Streams::stream)
                .findFirst();
    }

    public static TableId parseTableId(
            String rawTable,
            Optional<String> dataset,
            Optional<String> project) {
        Matcher matcher = QUALIFIED_TABLE_REGEX.matcher(rawTable);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    format("Invalid Table ID '%s'. Must match '%s'", rawTable, QUALIFIED_TABLE_REGEX));
        }
        String table = matcher.group(5);
        Optional<String> parsedDataset = Optional.ofNullable(matcher.group(4));
        Optional<String> parsedProject = Optional.ofNullable(matcher.group(3));

        String actualDataset = firstPresent(parsedDataset, dataset).orElseThrow(() ->
                new IllegalArgumentException("'dataset' not parsed or provided."));
        return firstPresent(parsedProject, project)
                .map(p -> TableId.of(p, actualDataset, table))
                .orElse(TableId.of(actualDataset, table));
    }
}
