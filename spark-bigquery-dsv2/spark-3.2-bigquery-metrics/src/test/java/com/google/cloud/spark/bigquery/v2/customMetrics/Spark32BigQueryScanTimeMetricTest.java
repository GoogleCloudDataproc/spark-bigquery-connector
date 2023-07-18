package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.Spark32BigQueryCustomMetricConstants.*;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class Spark32BigQueryScanTimeMetricTest {
  private final Spark32BigQueryScanTimeMetric spark32BigQueryScanTimeMetric =
      new Spark32BigQueryScanTimeMetric();

  @Test
  public void testName() {
    assertThat(spark32BigQueryScanTimeMetric.name()).isEqualTo(BIG_QUERY_SCAN_TIME_METRIC_NAME);
  }

  @Test
  public void testDescription() {
    assertThat(spark32BigQueryScanTimeMetric.description())
        .isEqualTo(BIG_QUERY_SCAN_TIME_METRIC_DESCRIPTION);
  }

  @Test
  public void testAggregateMetrics() {
    assertThat(spark32BigQueryScanTimeMetric.aggregateTaskMetrics(new long[] {1000L, 2000L}))
        .isEqualTo("3000");
  }
}
