package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.*;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class SparkBigQueryRowsReadMetricTest {
  private final SparkBigQueryRowsReadMetric sparkBigQueryRowsReadMetric =
      new SparkBigQueryRowsReadMetric();

  @Test
  public void testName() {
    assertThat(sparkBigQueryRowsReadMetric.name()).isEqualTo(BIG_QUERY_ROWS_READ_METRIC_NAME);
  }

  @Test
  public void testDescription() {
    assertThat(sparkBigQueryRowsReadMetric.description())
        .isEqualTo(BIG_QUERY_ROWS_READ_METRIC_DESCRIPTION);
  }

  @Test
  public void testAggregateMetrics() {
    assertThat(sparkBigQueryRowsReadMetric.aggregateTaskMetrics(new long[] {1000L, 2000L}))
        .isEqualTo("3000");
  }
}
