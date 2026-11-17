/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.spark.bigquery.v2;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_NUM_ROWS_BEFORE_PRUNING_METRIC_NAME;
import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_RAW_INPUT_ROWS_METRIC_NAME;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.inject.Injector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Test;

public class Spark34BigQueryScanBuilderTest {

  @Test
  public void testMetricsDeclaredAndReportedWhenKnown() throws Exception {
    BigQueryDataSourceReaderContext ctx = mock(BigQueryDataSourceReaderContext.class);
    when(ctx.getNumRowsBeforePruning()).thenReturn(OptionalLong.of(164_656L));
    Spark34BigQueryScanBuilder scanBuilder = new Spark34BigQueryScanBuilder(ctx);

    List<String> expectedNames =
        new ArrayList<>(metricNames(new Spark33BigQueryScanBuilder(ctx).supportedCustomMetrics()));
    expectedNames.add(BIG_QUERY_NUM_ROWS_BEFORE_PRUNING_METRIC_NAME);
    expectedNames.add(BIG_QUERY_RAW_INPUT_ROWS_METRIC_NAME);
    CustomMetric[] supported = scanBuilder.supportedCustomMetrics();
    assertThat(metricNames(supported)).containsExactlyElementsIn(expectedNames);

    for (CustomMetric metric : supported) {
      if (metric.name().equals(BIG_QUERY_NUM_ROWS_BEFORE_PRUNING_METRIC_NAME)
          || metric.name().equals(BIG_QUERY_RAW_INPUT_ROWS_METRIC_NAME)) {
        // SQLAppStatusListener instantiates CustomMetric classes via public 0-arg reflection.
        CustomMetric reflected =
            Class.forName(metric.getClass().getName())
                .asSubclass(CustomMetric.class)
                .getDeclaredConstructor()
                .newInstance();
        assertThat(reflected.aggregateTaskMetrics(new long[] {1_000L, 2_000L})).isEqualTo("3,000");
      }
    }

    assertThat(driverMetricValues(scanBuilder.reportDriverMetrics()))
        .containsExactly(
            BIG_QUERY_NUM_ROWS_BEFORE_PRUNING_METRIC_NAME,
            164_656L,
            BIG_QUERY_RAW_INPUT_ROWS_METRIC_NAME,
            164_656L);
  }

  @Test
  public void testMetricsOmittedWhenUnknown() {
    BigQueryDataSourceReaderContext ctx = mock(BigQueryDataSourceReaderContext.class);
    when(ctx.getNumRowsBeforePruning()).thenReturn(OptionalLong.empty());
    Spark34BigQueryScanBuilder scanBuilder = new Spark34BigQueryScanBuilder(ctx);

    assertThat(metricNames(scanBuilder.supportedCustomMetrics()))
        .containsExactlyElementsIn(
            metricNames(new Spark33BigQueryScanBuilder(ctx).supportedCustomMetrics()));
    assertThat(scanBuilder.reportDriverMetrics()).isEmpty();
  }

  @Test
  public void testSpark34BigQueryTableCreatesSpark34BigQueryScanBuilder() {
    BigQueryDataSourceReaderContext ctx = mock(BigQueryDataSourceReaderContext.class);
    when(ctx.getNumRowsBeforePruning()).thenReturn(OptionalLong.of(164_656L));
    Injector injector = mock(Injector.class);
    SparkBigQueryConfig config = mock(SparkBigQueryConfig.class);
    when(config.getQuery()).thenReturn(Optional.empty());
    when(config.getTableId()).thenReturn(TableId.of("project", "dataset", "table"));
    when(injector.getInstance(SparkBigQueryConfig.class)).thenReturn(config);
    StructType schema = new StructType().add("name", DataTypes.StringType, /* nullable= */ true);
    Spark34BigQueryTable table =
        new Spark34BigQueryTable(injector, () -> schema) {
          @Override
          protected BigQueryDataSourceReaderContext createBigQueryDataSourceReaderContext(
              CaseInsensitiveStringMap options) {
            return ctx;
          }
        };

    ScanBuilder scanBuilder = table.newScanBuilder(CaseInsensitiveStringMap.empty());

    assertThat(scanBuilder).isInstanceOf(Spark34BigQueryScanBuilder.class);
    assertThat(driverMetricValues(((Spark34BigQueryScanBuilder) scanBuilder).reportDriverMetrics()))
        .containsExactly(
            BIG_QUERY_NUM_ROWS_BEFORE_PRUNING_METRIC_NAME,
            164_656L,
            BIG_QUERY_RAW_INPUT_ROWS_METRIC_NAME,
            164_656L);
  }

  private static List<String> metricNames(CustomMetric[] metrics) {
    return Arrays.stream(metrics).map(CustomMetric::name).collect(Collectors.toList());
  }

  private static Map<String, Long> driverMetricValues(CustomTaskMetric[] metrics) {
    return Arrays.stream(metrics)
        .collect(Collectors.toMap(CustomTaskMetric::name, CustomTaskMetric::value));
  }
}
