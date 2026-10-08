/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2.context;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.MaterializedViewDefinition;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.SnapshotTableDefinition;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryErrorCode;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfigBuilder;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class BigQueryDataSourceReaderContextTest {

  private static final Schema SCHEMA = Schema.of(Field.of("name", StandardSQLTypeName.STRING));
  private static final TableId TABLE_ID = TableId.of("project", "dataset", "table");
  private static final TableInfo TABLE_INFO =
      TableInfo.of(TABLE_ID, StandardTableDefinition.of(SCHEMA));

  @Test
  public void testEmptyProjectionPartitionsLargerThanIntMax() {
    long rowCount = 13_927_954_135L;
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    when(bigQueryClient.calculateTableSize(any(TableId.class), any(), any(), anyBoolean()))
        .thenReturn(rowCount);

    BigQueryDataSourceReaderContext ctx =
        createEmptyProjectionContext(bigQueryClient, mock(SparkBigQueryConfig.class));

    List<Long> partitionSizes =
        ctx.planInputPartitionContexts()
            .map(partition -> ((EmptyProjectionInputPartitionContext) partition).partitionSize)
            .collect(Collectors.toList());

    assertThat(partitionSizes).containsExactly(6_963_977_068L, 6_963_977_067L).inOrder();
    assertThat(partitionSizes.stream().mapToLong(Long::longValue).sum()).isEqualTo(rowCount);
  }

  @Test
  public void testNumRowsBeforePruningForTableAndSnapshot() {
    assertThat(numRowsBeforePruning(mockTable(StandardTableDefinition.of(SCHEMA), 164_656L)))
        .hasValue(164_656L);
    SnapshotTableDefinition snapshot =
        SnapshotTableDefinition.newBuilder().setSchema(SCHEMA).setBaseTableId(TABLE_ID).build();
    assertThat(numRowsBeforePruning(mockTable(snapshot, 42_000L))).hasValue(42_000L);
  }

  @Test
  public void testNumRowsBeforePruningWithStreamingBuffer() {
    StandardTableDefinition withEstimate = tableWithStreamingBuffer(250L);
    assertThat(numRowsBeforePruning(mockTable(withEstimate, 1_000L))).hasValue(1_250L);
    assertThat(numRowsBeforePruning(mockTable(withEstimate, 0L))).hasValue(250L);
    assertThat(numRowsBeforePruning(mockTable(withEstimate, (BigInteger) null))).hasValue(250L);
    assertThat(numRowsBeforePruning(mockTable(tableWithStreamingBuffer(null), 1_000L)))
        .hasValue(1_000L);
    assertThat(numRowsBeforePruning(mockTable(tableWithStreamingBuffer(-10L), 1_000L)))
        .hasValue(1_000L);
    assertThat(
            numRowsBeforePruning(
                mockTable(
                    withEstimate, BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.ONE))))
        .isEmpty();
  }

  @Test
  public void testNumRowsBeforePruningOmittedCases() {
    ReadSessionCreatorConfig timeTravelConfig =
        new ReadSessionCreatorConfigBuilder()
            .setDefaultParallelism(2)
            .setSnapshotTimeMillis(OptionalLong.of(1_700_000_000_000L))
            .build();
    assertThat(
            numRowsBeforePruning(
                mockTable(StandardTableDefinition.of(SCHEMA), 164_656L), timeTravelConfig))
        .isEmpty();
    assertThat(
            numRowsBeforePruning(
                mockTable(
                    ViewDefinition.newBuilder("SELECT 'a' AS name").setSchema(SCHEMA).build(),
                    100L)))
        .isEmpty();
    assertThat(
            numRowsBeforePruning(
                mockTable(
                    MaterializedViewDefinition.newBuilder("SELECT 'a' AS name")
                        .setSchema(SCHEMA)
                        .build(),
                    100L)))
        .isEmpty();
    assertThat(
            numRowsBeforePruning(
                mockTable(
                    ExternalTableDefinition.of(
                        "gs://bucket/file.parquet", SCHEMA, FormatOptions.parquet()),
                    100L)))
        .isEmpty();
    assertThat(numRowsBeforePruning(mockTable(StandardTableDefinition.of(SCHEMA), 0L))).isEmpty();
    assertThat(
            numRowsBeforePruning(mockTable(StandardTableDefinition.of(SCHEMA), (BigInteger) null)))
        .isEmpty();
  }

  private static TableInfo mockTable(TableDefinition definition, long numRows) {
    return mockTable(definition, BigInteger.valueOf(numRows));
  }

  private static TableInfo mockTable(TableDefinition definition, BigInteger numRows) {
    TableInfo table = mock(TableInfo.class);
    when(table.getTableId()).thenReturn(TABLE_ID);
    when(table.getDefinition()).thenReturn(definition);
    when(table.getNumRows()).thenReturn(numRows);
    return table;
  }

  private static StandardTableDefinition tableWithStreamingBuffer(Long estimatedRows) {
    StandardTableDefinition.StreamingBuffer streamingBuffer =
        mock(StandardTableDefinition.StreamingBuffer.class);
    when(streamingBuffer.getEstimatedRows()).thenReturn(estimatedRows);
    return StandardTableDefinition.newBuilder()
        .setSchema(SCHEMA)
        .setStreamingBuffer(streamingBuffer)
        .build();
  }

  private static OptionalLong numRowsBeforePruning(TableInfo table) {
    return numRowsBeforePruning(
        table, new ReadSessionCreatorConfigBuilder().setDefaultParallelism(2).build());
  }

  private static OptionalLong numRowsBeforePruning(
      TableInfo table, ReadSessionCreatorConfig readSessionCreatorConfig) {
    return new BigQueryDataSourceReaderContext(
            table,
            mock(BigQueryClient.class),
            /* bigQueryReadClientFactory= */ null,
            /* tracerFactory= */ null,
            readSessionCreatorConfig,
            /* globalFilter= */ Optional.empty(),
            /* schema= */ Optional.of(new StructType()),
            "applicationId",
            mock(SparkBigQueryConfig.class),
            /* sqlContext= */ null,
            /* sparkSession= */ null,
            /* readTableOptions= */ null)
        .getNumRowsBeforePruning();
  }

  @Test
  public void testEmptyProjectionPassesAllowStaleCountFromMetadata() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    when(bigQueryClient.calculateTableSize(any(TableId.class), any(), any(), anyBoolean()))
        .thenReturn(10L);
    SparkBigQueryConfig options = mock(SparkBigQueryConfig.class);
    when(options.isAllowStaleCountFromMetadata()).thenReturn(true);

    createEmptyProjectionContext(bigQueryClient, options)
        .planInputPartitionContexts()
        .collect(Collectors.toList());

    verify(bigQueryClient)
        .calculateTableSize(eq(TABLE_ID), eq(Optional.empty()), eq(OptionalLong.empty()), eq(true));
  }

  @Test
  public void testEmptyProjectionAccessDeniedThrowsActionableError() {
    String message = "User does not have bigquery.jobs.create permission in project p.";
    BigQueryException accessDenied =
        new BigQueryException(403, message, new BigQueryError("accessDenied", "global", message));
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    when(bigQueryClient.calculateTableSize(any(TableId.class), any(), any(), anyBoolean()))
        .thenThrow(accessDenied);
    when(bigQueryClient.getProjectId()).thenReturn("parent-project");

    BigQueryDataSourceReaderContext ctx =
        createEmptyProjectionContext(bigQueryClient, mock(SparkBigQueryConfig.class));
    BigQueryConnectorException e =
        assertThrows(
            BigQueryConnectorException.class,
            () -> ctx.planInputPartitionContexts().collect(Collectors.toList()));

    assertThat(e.getErrorCode()).isEqualTo(BigQueryErrorCode.BIGQUERY_FAILED_TO_EXECUTE_QUERY);
    assertThat(e).hasMessageThat().contains("bigquery.jobs.create");
    assertThat(e).hasMessageThat().contains("project parent-project");
    assertThat(e).hasMessageThat().contains("allowStaleCountFromMetadata");
    assertThat(e).hasMessageThat().doesNotContain("optimizedEmptyProjection");
    assertThat(e).hasCauseThat().isSameInstanceAs(accessDenied);
  }

  @Test
  public void testEmptyProjectionOtherErrorsArePropagated() {
    BigQueryException quotaExceeded =
        new BigQueryException(403, "quota", new BigQueryError("quotaExceeded", "global", "quota"));
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    when(bigQueryClient.calculateTableSize(any(TableId.class), any(), any(), anyBoolean()))
        .thenThrow(quotaExceeded);

    BigQueryDataSourceReaderContext ctx =
        createEmptyProjectionContext(bigQueryClient, mock(SparkBigQueryConfig.class));
    BigQueryException e =
        assertThrows(
            BigQueryException.class,
            () -> ctx.planInputPartitionContexts().collect(Collectors.toList()));

    assertThat(e).isSameInstanceAs(quotaExceeded);
  }

  private static BigQueryDataSourceReaderContext createEmptyProjectionContext(
      BigQueryClient bigQueryClient, SparkBigQueryConfig options) {
    return new BigQueryDataSourceReaderContext(
        TABLE_INFO,
        bigQueryClient,
        /* bigQueryReadClientFactory= */ null,
        /* tracerFactory= */ null,
        new ReadSessionCreatorConfigBuilder().setDefaultParallelism(2).build(),
        /* globalFilter= */ Optional.empty(),
        /* schema= */ Optional.of(new StructType()),
        "applicationId",
        options,
        /* sqlContext= */ null,
        /* sparkSession= */ null,
        /* readTableOptions= */ null);
  }
}
