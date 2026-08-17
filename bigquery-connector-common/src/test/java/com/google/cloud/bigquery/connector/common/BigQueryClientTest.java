/*
 * Copyright 2026 Google Inc. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.connector.common.BigQueryClient.LoadDataOptions;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.Test;

public class BigQueryClientTest {

  private static final TableId TABLE_ID = TableId.of("project", "dataset", "table");
  private static final Schema SCHEMA =
      Schema.of(
          Field.of("event_ts", LegacySQLTypeName.TIMESTAMP),
          Field.of("other_ts", LegacySQLTypeName.TIMESTAMP),
          Field.of("range_key", LegacySQLTypeName.INTEGER),
          Field.of("cluster_key", LegacySQLTypeName.STRING));

  @Test
  public void validateDestinationTableLayout_acceptsMatchingTimeOptionsWithDefaultType() {
    TableInfo destination =
        tableInfo(
            TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                .setField("event_ts")
                .setExpirationMs(86_400_000L)
                .build(),
            null,
            ImmutableList.of());

    BigQueryClient.validateDestinationTableLayout(
        destination, options("event_ts", null, null, 86_400_000L, null));
  }

  @Test
  public void validateDestinationTableLayout_acceptsOmittedLayoutOptions() {
    TableInfo destination =
        tableInfo(
            TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField("event_ts").build(),
            null,
            ImmutableList.of("cluster_key"));

    BigQueryClient.validateDestinationTableLayout(destination, emptyOptions());
  }

  @Test
  public void validateDestinationTableLayout_acceptsMatchingRangeClusteringAndFilterOptions() {
    RangePartitioning.Range destinationRange = range(0, 100, 10);
    TableInfo destination =
        tableInfo(
                null,
                RangePartitioning.newBuilder()
                    .setField("range_key")
                    .setRange(destinationRange)
                    .build(),
                ImmutableList.of("cluster_key"))
            .toBuilder()
            .setRequirePartitionFilter(true)
            .build();

    BigQueryClient.validateDestinationTableLayout(
        destination, options("range_key", null, range(0, 100, 10), null, true, "cluster_key"));
  }

  @Test
  public void validateDestinationTableLayout_rejectsMismatchedOptions() {
    TableInfo timeDestination =
        tableInfo(
            TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                .setField("event_ts")
                .setExpirationMs(86_400_000L)
                .setRequirePartitionFilter(false)
                .build(),
            null,
            ImmutableList.of("cluster_key", "range_key"));
    TableInfo rangeDestination =
        tableInfo(
            null,
            RangePartitioning.newBuilder()
                .setField("range_key")
                .setRange(range(0, 100, 10))
                .build(),
            ImmutableList.of());

    assertLayoutMismatch(
        timeDestination,
        options("event_ts", TimePartitioning.Type.HOUR, null, null, null),
        "partitionType");
    assertLayoutMismatch(
        timeDestination, options(null, null, null, 172_800_000L, null), "partitionExpirationMs");
    assertLayoutMismatch(
        timeDestination, options(null, null, null, null, true), "partitionRequireFilter");
    assertLayoutMismatch(
        timeDestination,
        options(null, null, null, null, null, "range_key", "cluster_key"),
        "clusteredFields");
    assertLayoutMismatch(
        rangeDestination,
        options("range_key", null, range(0, 100, 20), null, null),
        "partitionRange");
    assertLayoutMismatch(
        timeDestination,
        options("range_key", null, range(0, 100, 10), null, null),
        "partitioning type");
  }

  @Test
  public void validateDestinationTableLayout_rejectsMixedTimeAndRangeOptions() {
    TableInfo destination =
        tableInfo(
            null,
            RangePartitioning.newBuilder()
                .setField("range_key")
                .setRange(range(0, 100, 10))
                .build(),
            ImmutableList.of());

    assertLayoutMismatch(
        destination,
        options("range_key", TimePartitioning.Type.DAY, range(0, 100, 10), null, null),
        "range partitioning options cannot be combined with time partitioning options");
  }

  @Test
  public void validateDestinationTableLayout_rejectsIngestionTimePartitioning() {
    TableInfo destination =
        tableInfo(
            TimePartitioning.newBuilder(TimePartitioning.Type.DAY).build(),
            null,
            ImmutableList.of());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> BigQueryClient.validateDestinationTableLayout(destination, emptyOptions()));

    assertThat(exception)
        .hasMessageThat()
        .contains("does not support ingestion-time partitioned destination table");
  }

  private static TableInfo tableInfo(
      TimePartitioning timePartitioning,
      RangePartitioning rangePartitioning,
      ImmutableList<String> clusteredFields) {
    StandardTableDefinition.Builder definition =
        StandardTableDefinition.newBuilder().setSchema(SCHEMA);
    if (timePartitioning != null) {
      definition.setTimePartitioning(timePartitioning);
    }
    if (rangePartitioning != null) {
      definition.setRangePartitioning(rangePartitioning);
    }
    if (!clusteredFields.isEmpty()) {
      definition.setClustering(Clustering.newBuilder().setFields(clusteredFields).build());
    }
    return TableInfo.of(TABLE_ID, definition.build());
  }

  private static RangePartitioning.Range range(long start, long end, long interval) {
    return RangePartitioning.Range.newBuilder()
        .setStart(start)
        .setEnd(end)
        .setInterval(interval)
        .build();
  }

  private static DestinationValidationOptions emptyOptions() {
    return options(null, null, null, null, null);
  }

  private static DestinationValidationOptions options(
      String partitionField,
      TimePartitioning.Type partitionType,
      RangePartitioning.Range partitionRange,
      Long partitionExpirationMs,
      Boolean partitionRequireFilter,
      String... clusteredFields) {
    LoadDataOptions options = mock(LoadDataOptions.class);
    when(options.getTableId()).thenReturn(TABLE_ID);
    when(options.getPartitionField()).thenReturn(Optional.ofNullable(partitionField));
    when(options.getPartitionType()).thenReturn(Optional.ofNullable(partitionType));
    when(options.getPartitionRange()).thenReturn(Optional.ofNullable(partitionRange));
    when(options.getPartitionExpirationMs())
        .thenReturn(
            partitionExpirationMs == null
                ? OptionalLong.empty()
                : OptionalLong.of(partitionExpirationMs));
    when(options.getPartitionRequireFilter())
        .thenReturn(Optional.ofNullable(partitionRequireFilter));
    when(options.getClusteredFields())
        .thenReturn(
            clusteredFields.length == 0
                ? Optional.empty()
                : Optional.of(ImmutableList.copyOf(clusteredFields)));
    return DestinationValidationOptions.from(options);
  }

  private static void assertLayoutMismatch(
      TableInfo destination, DestinationValidationOptions options, String expectedMessage) {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> BigQueryClient.validateDestinationTableLayout(destination, options));
    assertThat(exception).hasMessageThat().contains(expectedMessage);
  }
}
