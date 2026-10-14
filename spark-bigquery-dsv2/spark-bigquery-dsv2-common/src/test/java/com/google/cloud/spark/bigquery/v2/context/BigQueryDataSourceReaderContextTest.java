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

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigquery.BigLakeConfiguration;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfigBuilder;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStub;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class BigQueryDataSourceReaderContextTest {

  // Fixtures for the estimateStatistics() tests, which cover the table size the connector reports
  // to Spark for BigLake tables. Spark broadcasts a join side (which Dynamic Partition Pruning
  // uses) when its reported size is below spark.sql.autoBroadcastJoinThreshold, and uses a default
  // (very large) size when none is reported.

  private static final Schema TWO_INT64_COLUMNS_SCHEMA =
      Schema.of(
          Field.of("id", StandardSQLTypeName.INT64),
          Field.of("date_sk", StandardSQLTypeName.INT64));
  // Row sizes as computed by the connector (mirroring Spark's EstimationUtils.getSizePerRow):
  // 8 bytes of row overhead plus the default size of each column (8 bytes for INT64/LongType).
  private static final long TWO_INT64_COLUMNS_ROW_SIZE = 24L;
  private static final long ONE_INT64_COLUMN_ROW_SIZE = 16L;

  // A BigLake managed table (BigQuery table for Apache Iceberg): a TABLE whose definition carries a
  // BigLakeConfiguration. The read session provides a row count estimate for these tables.
  private static final TableInfo BIGLAKE_MANAGED_TABLE =
      TableInfo.of(
          TableId.of("project", "dataset", "biglake_managed_table"),
          StandardTableDefinition.newBuilder()
              .setSchema(TWO_INT64_COLUMNS_SCHEMA)
              .setBigLakeConfiguration(
                  BigLakeConfiguration.newBuilder()
                      .setTableFormat("ICEBERG")
                      .setConnectionId("project.us.connection")
                      .setFileFormat("PARQUET")
                      .setStorageUri("gs://bucket/table")
                      .build())
              .build());

  // A BigLake external table. It shares the estimateStatistics() code path with BigLake managed
  // tables, but its row count is not exposed to Spark.
  private static final TableInfo EXTERNAL_TABLE =
      TableInfo.of(
          TableId.of("project", "dataset", "external_table"),
          ExternalTableDefinition.newBuilder(
                  "gs://bucket/table/*.parquet", TWO_INT64_COLUMNS_SCHEMA, FormatOptions.parquet())
              .build());

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

  /**
   * When BigQuery reports a physical file size, it is used as the size estimate (scaled by the
   * projected row size, which here equals the full row size). The physical file size takes
   * precedence over the row-count estimate.
   */
  @Test
  public void testBigLakeManagedTableStatisticsUsePhysicalFileSize() throws Exception {
    BigQueryDataSourceReaderContext ctx =
        createContextWithReadSession(
            BIGLAKE_MANAGED_TABLE,
            ReadSession.newBuilder()
                .setEstimatedTotalPhysicalFileSize(5_000L)
                .setEstimatedRowCount(1_000L)
                .build());

    StatisticsContext statistics = ctx.estimateStatistics();

    // 5_000 bytes (the physical size), not 1_000 rows * 24 bytes (the row-count estimate).
    assertThat(statistics.sizeInBytes()).isEqualTo(OptionalLong.of(5_000L));
    assertThat(statistics.numRows()).isEqualTo(OptionalLong.of(1_000L));
  }

  /**
   * When the read session reports a physical file size of 0 and a positive row count, the size is
   * estimated as row count * row size.
   */
  @Test
  public void testBigLakeManagedTableStatisticsFallBackToRowCount() throws Exception {
    BigQueryDataSourceReaderContext ctx =
        createContextWithReadSession(
            BIGLAKE_MANAGED_TABLE,
            ReadSession.newBuilder()
                .setEstimatedTotalPhysicalFileSize(0L)
                .setEstimatedRowCount(1_000L)
                .build());

    StatisticsContext statistics = ctx.estimateStatistics();

    assertThat(statistics.sizeInBytes())
        .isEqualTo(OptionalLong.of(1_000L * TWO_INT64_COLUMNS_ROW_SIZE));
    assertThat(statistics.numRows()).isEqualTo(OptionalLong.of(1_000L));
  }

  /**
   * The row-count estimate uses the projected row size, so selecting fewer columns yields a smaller
   * estimate, as with the physical-size estimate.
   */
  @Test
  public void testBigLakeManagedTableStatisticsFallBackToRowCountWithProjection() throws Exception {
    BigQueryDataSourceReaderContext ctx =
        createContextWithReadSession(
            BIGLAKE_MANAGED_TABLE,
            ReadSession.newBuilder()
                .setEstimatedTotalPhysicalFileSize(0L)
                .setEstimatedRowCount(1_000L)
                .build());
    // Simulate Spark pruning the read schema down to a single column.
    ctx.pruneColumns(new StructType().add("date_sk", DataTypes.LongType));

    StatisticsContext statistics = ctx.estimateStatistics();

    assertThat(statistics.sizeInBytes())
        .isEqualTo(OptionalLong.of(1_000L * ONE_INT64_COLUMN_ROW_SIZE));
  }

  /**
   * For very large row counts, row count * row size exceeds the range of a long. The estimate
   * saturates at Long.MAX_VALUE.
   */
  @Test
  public void testBigLakeManagedTableStatisticsFallBackSaturatesOnOverflow() throws Exception {
    BigQueryDataSourceReaderContext ctx =
        createContextWithReadSession(
            BIGLAKE_MANAGED_TABLE,
            ReadSession.newBuilder()
                .setEstimatedTotalPhysicalFileSize(0L)
                .setEstimatedRowCount(Long.MAX_VALUE / 2)
                .build());

    StatisticsContext statistics = ctx.estimateStatistics();

    assertThat(statistics.sizeInBytes()).isEqualTo(OptionalLong.of(Long.MAX_VALUE));
  }

  /**
   * With neither a physical file size nor a row count, no size estimate is reported (rather than a
   * size of 0).
   */
  @Test
  public void testBigLakeManagedTableStatisticsUnknownWithoutSizeOrRowCount() throws Exception {
    BigQueryDataSourceReaderContext ctx =
        createContextWithReadSession(
            BIGLAKE_MANAGED_TABLE,
            ReadSession.newBuilder()
                .setEstimatedTotalPhysicalFileSize(0L)
                .setEstimatedRowCount(0L)
                .build());

    StatisticsContext statistics = ctx.estimateStatistics();

    assertThat(statistics.sizeInBytes()).isEqualTo(OptionalLong.empty());
  }

  /**
   * The row-count estimate applies to BigLake managed tables only. For external tables, which share
   * the same code path, no size estimate is reported when no physical file size is available.
   */
  @Test
  public void testExternalTableStatisticsDoNotFallBackToRowCount() throws Exception {
    BigQueryDataSourceReaderContext ctx =
        createContextWithReadSession(
            EXTERNAL_TABLE,
            ReadSession.newBuilder()
                .setEstimatedTotalPhysicalFileSize(0L)
                .setEstimatedRowCount(1_000L)
                .build());

    StatisticsContext statistics = ctx.estimateStatistics();

    assertThat(statistics.sizeInBytes()).isEqualTo(OptionalLong.empty());
    assertThat(statistics.numRows()).isEqualTo(OptionalLong.empty());
  }

  /**
   * For very large physical file sizes, the size is scaled by the projected row size without the
   * intermediate value exceeding the range of a long.
   */
  @Test
  public void testBigLakeManagedTableStatisticsLargePhysicalFileSizeWithProjection()
      throws Exception {
    long physicalFileSize = Long.MAX_VALUE / 2;
    BigQueryDataSourceReaderContext ctx =
        createContextWithReadSession(
            BIGLAKE_MANAGED_TABLE,
            ReadSession.newBuilder()
                .setEstimatedTotalPhysicalFileSize(physicalFileSize)
                .setEstimatedRowCount(1_000L)
                .build());
    ctx.pruneColumns(new StructType().add("date_sk", DataTypes.LongType));

    StatisticsContext statistics = ctx.estimateStatistics();

    long expectedSize =
        BigInteger.valueOf(physicalFileSize)
            .multiply(BigInteger.valueOf(ONE_INT64_COLUMN_ROW_SIZE))
            .divide(BigInteger.valueOf(TWO_INT64_COLUMNS_ROW_SIZE))
            .longValueExact();
    assertThat(statistics.sizeInBytes()).isEqualTo(OptionalLong.of(expectedSize));
  }

  /**
   * A negative physical file size is treated as unavailable, so the row-count estimate applies to
   * BigLake managed tables.
   */
  @Test
  public void testBigLakeManagedTableStatisticsNegativePhysicalFileSize() throws Exception {
    BigQueryDataSourceReaderContext ctx =
        createContextWithReadSession(
            BIGLAKE_MANAGED_TABLE,
            ReadSession.newBuilder()
                .setEstimatedTotalPhysicalFileSize(-1L)
                .setEstimatedRowCount(1_000L)
                .build());

    StatisticsContext statistics = ctx.estimateStatistics();

    assertThat(statistics.sizeInBytes())
        .isEqualTo(OptionalLong.of(1_000L * TWO_INT64_COLUMNS_ROW_SIZE));
    assertThat(statistics.numRows()).isEqualTo(OptionalLong.of(1_000L));
  }

  /** A negative row count is treated as unavailable for both the size and the row count. */
  @Test
  public void testBigLakeManagedTableStatisticsNegativeRowCount() throws Exception {
    BigQueryDataSourceReaderContext ctx =
        createContextWithReadSession(
            BIGLAKE_MANAGED_TABLE,
            ReadSession.newBuilder()
                .setEstimatedTotalPhysicalFileSize(0L)
                .setEstimatedRowCount(-1L)
                .build());

    StatisticsContext statistics = ctx.estimateStatistics();

    assertThat(statistics.sizeInBytes()).isEqualTo(OptionalLong.empty());
    assertThat(statistics.numRows()).isEqualTo(OptionalLong.empty());
  }

  /** For external tables, a negative physical file size results in no size estimate. */
  @Test
  public void testExternalTableStatisticsNegativePhysicalFileSize() throws Exception {
    BigQueryDataSourceReaderContext ctx =
        createContextWithReadSession(
            EXTERNAL_TABLE,
            ReadSession.newBuilder()
                .setEstimatedTotalPhysicalFileSize(-1L)
                .setEstimatedRowCount(1_000L)
                .build());

    StatisticsContext statistics = ctx.estimateStatistics();

    assertThat(statistics.sizeInBytes()).isEqualTo(OptionalLong.empty());
  }

  /** scaleSaturated() computes value * numerator / denominator exactly, rounded toward zero. */
  @Test
  public void testScaleSaturated() {
    assertThat(BigQueryDataSourceReaderContext.scaleSaturated(5_000L, 24L, 24L)).isEqualTo(5_000L);
    assertThat(BigQueryDataSourceReaderContext.scaleSaturated(3_000L, 16L, 24L)).isEqualTo(2_000L);
    assertThat(BigQueryDataSourceReaderContext.scaleSaturated(7L, 1L, 3L)).isEqualTo(2L);
    assertThat(BigQueryDataSourceReaderContext.scaleSaturated(0L, 16L, 24L)).isEqualTo(0L);
  }

  /**
   * scaleSaturated() returns the exact result when it fits in a long, even if value * numerator
   * does not.
   */
  @Test
  public void testScaleSaturatedLargeIntermediateProduct() {
    assertThat(
            BigQueryDataSourceReaderContext.scaleSaturated(
                3_500_000_000L, 3_000_000_000L, 4_000_000_000L))
        .isEqualTo(2_625_000_000L);
  }

  /** scaleSaturated() saturates when the result exceeds the range of a long. */
  @Test
  public void testScaleSaturatedSaturatesOnOverflow() {
    assertThat(BigQueryDataSourceReaderContext.scaleSaturated(Long.MAX_VALUE / 2, 3L, 1L))
        .isEqualTo(Long.MAX_VALUE);
    assertThat(BigQueryDataSourceReaderContext.scaleSaturated(Long.MIN_VALUE / 2, 3L, 1L))
        .isEqualTo(Long.MIN_VALUE);
  }

  /**
   * Creates a reader context for {@code tableInfo} whose read session creation returns {@code
   * readSession}, so that estimateStatistics() sees the given size and row count estimates.
   */
  @SuppressWarnings("unchecked")
  private static BigQueryDataSourceReaderContext createContextWithReadSession(
      TableInfo tableInfo, ReadSession readSession) throws Exception {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    when(bigQueryClient.getTable(any())).thenReturn(tableInfo);

    // BigQueryReadClient's methods are final and can't be mocked directly, so mock the stub it
    // delegates to instead.
    UnaryCallable<CreateReadSessionRequest, ReadSession> createReadSessionCallable =
        mock(UnaryCallable.class);
    when(createReadSessionCallable.call(any(CreateReadSessionRequest.class)))
        .thenReturn(readSession);
    EnhancedBigQueryReadStub stub = mock(EnhancedBigQueryReadStub.class);
    when(stub.createReadSessionCallable()).thenReturn(createReadSessionCallable);
    BigQueryReadClient readClient = BigQueryReadClient.create(stub);
    // BigQueryReadClient.create(stub) leaves the settings null, which causes an NPE when creating
    // a read session.
    java.lang.reflect.Field settingsField = BigQueryReadClient.class.getDeclaredField("settings");
    settingsField.setAccessible(true);
    settingsField.set(readClient, BigQueryReadSettings.newBuilder().build());
    BigQueryClientFactory bigQueryReadClientFactory = mock(BigQueryClientFactory.class);
    when(bigQueryReadClientFactory.getBigQueryReadClient()).thenReturn(readClient);

    return new BigQueryDataSourceReaderContext(
        tableInfo,
        bigQueryClient,
        bigQueryReadClientFactory,
        /* tracerFactory= */ null,
        new ReadSessionCreatorConfigBuilder().setEnableReadSessionCaching(false).build(),
        /* globalFilter= */ Optional.empty(),
        /* schema= */ Optional.empty(),
        "applicationId",
        mock(SparkBigQueryConfig.class),
        /* sqlContext= */ null,
        /* sparkSession= */ null,
        /* readTableOptions= */ null);
  }
}
