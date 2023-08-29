package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.*;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class SparkBigQueryScanTimeMetricTest {
  private final SparkBigQueryScanTimeMetric sparkBigQueryScanTimeMetric =
      new SparkBigQueryScanTimeMetric();

  @Test
  public void testName() {
    assertThat(sparkBigQueryScanTimeMetric.name()).isEqualTo(BIG_QUERY_SCAN_TIME_METRIC_NAME);
  }

  @Test
  public void testDescription() {
    assertThat(sparkBigQueryScanTimeMetric.description())
        .isEqualTo(BIG_QUERY_SCAN_TIME_METRIC_DESCRIPTION);
  }

  @Test
  public void testAggregateMetrics() {
    assertThat(sparkBigQueryScanTimeMetric.aggregateTaskMetrics(new long[] {1010L, 2020L, 3030L}))
        .isEqualTo("total (min, med, max)\n" + "6.1 s (1.0 s, 2.0 s, 3.0 s)");
  }
}
