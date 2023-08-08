package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_BYTES_READ_METRIC_DESCRIPTION;
import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_BYTES_READ_METRIC_NAME;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class SparkBigQueryBytesReadMetricTest {
  private final SparkBigQueryBytesReadMetric sparkBigQueryBytesReadMetric =
      new SparkBigQueryBytesReadMetric();

  @Test
  public void testName() {
    assertThat(sparkBigQueryBytesReadMetric.name()).isEqualTo(BIG_QUERY_BYTES_READ_METRIC_NAME);
  }

  @Test
  public void testDescription() {
    assertThat(sparkBigQueryBytesReadMetric.description())
        .isEqualTo(BIG_QUERY_BYTES_READ_METRIC_DESCRIPTION);
  }

  @Test
  public void testAggregateMetrics() {
    assertThat(sparkBigQueryBytesReadMetric.aggregateTaskMetrics(new long[] {1000L, 2000L}))
        .isEqualTo("3000");
  }
}
