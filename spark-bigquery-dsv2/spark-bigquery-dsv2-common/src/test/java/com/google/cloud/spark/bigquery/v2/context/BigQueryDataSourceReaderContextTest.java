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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

  @Test
  public void testEmptyProjectionPartitionsLargerThanIntMax() {
    long rowCount = 13_927_954_135L;
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    when(bigQueryClient.calculateTableSize(any(TableId.class), any())).thenReturn(rowCount);
    TableInfo tableInfo =
        TableInfo.of(
            TableId.of("project", "dataset", "table"),
            StandardTableDefinition.of(Schema.of(Field.of("name", StandardSQLTypeName.STRING))));

    BigQueryDataSourceReaderContext ctx =
        new BigQueryDataSourceReaderContext(
            tableInfo,
            bigQueryClient,
            /* bigQueryReadClientFactory= */ null,
            /* tracerFactory= */ null,
            new ReadSessionCreatorConfigBuilder().setDefaultParallelism(2).build(),
            /* globalFilter= */ Optional.empty(),
            /* schema= */ Optional.of(new StructType()),
            "applicationId",
            mock(SparkBigQueryConfig.class),
            /* sqlContext= */ null,
            /* sparkSession= */ null,
            /* readTableOptions= */ null);

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
        .isEqualTo(OptionalLong.of(164_656L));
    SnapshotTableDefinition snapshot =
        SnapshotTableDefinition.newBuilder().setSchema(SCHEMA).setBaseTableId(TABLE_ID).build();
    assertThat(numRowsBeforePruning(mockTable(snapshot, 42_000L)))
        .isEqualTo(OptionalLong.of(42_000L));
  }

  @Test
  public void testNumRowsBeforePruningWithStreamingBuffer() {
    StandardTableDefinition withEstimate = tableWithStreamingBuffer(250L);
    assertThat(numRowsBeforePruning(mockTable(withEstimate, 1_000L)))
        .isEqualTo(OptionalLong.of(1_250L));
    assertThat(numRowsBeforePruning(mockTable(withEstimate, 0L))).isEqualTo(OptionalLong.of(250L));
    assertThat(numRowsBeforePruning(mockTable(withEstimate, (BigInteger) null)))
        .isEqualTo(OptionalLong.of(250L));
    assertThat(numRowsBeforePruning(mockTable(tableWithStreamingBuffer(null), 1_000L)))
        .isEqualTo(OptionalLong.of(1_000L));
    assertThat(numRowsBeforePruning(mockTable(tableWithStreamingBuffer(-10L), 1_000L)))
        .isEqualTo(OptionalLong.of(1_000L));
    assertThat(
            numRowsBeforePruning(
                mockTable(
                    withEstimate, BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.ONE))))
        .isEqualTo(OptionalLong.empty());
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
        .isEqualTo(OptionalLong.empty());
    assertThat(
            numRowsBeforePruning(
                mockTable(
                    ViewDefinition.newBuilder("SELECT 'a' AS name").setSchema(SCHEMA).build(),
                    100L)))
        .isEqualTo(OptionalLong.empty());
    assertThat(
            numRowsBeforePruning(
                mockTable(
                    MaterializedViewDefinition.newBuilder("SELECT 'a' AS name")
                        .setSchema(SCHEMA)
                        .build(),
                    100L)))
        .isEqualTo(OptionalLong.empty());
    assertThat(
            numRowsBeforePruning(
                mockTable(
                    ExternalTableDefinition.of(
                        "gs://bucket/file.parquet", SCHEMA, FormatOptions.parquet()),
                    100L)))
        .isEqualTo(OptionalLong.empty());
    assertThat(numRowsBeforePruning(mockTable(StandardTableDefinition.of(SCHEMA), 0L)))
        .isEqualTo(OptionalLong.empty());
    assertThat(
            numRowsBeforePruning(mockTable(StandardTableDefinition.of(SCHEMA), (BigInteger) null)))
        .isEqualTo(OptionalLong.empty());
    TableInfo nullDefAfterInit = mockTable(StandardTableDefinition.of(SCHEMA), 100L);
    when(nullDefAfterInit.getDefinition())
        .thenReturn(StandardTableDefinition.of(SCHEMA), (TableDefinition) null);
    assertThat(numRowsBeforePruning(nullDefAfterInit)).isEqualTo(OptionalLong.empty());
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
}
