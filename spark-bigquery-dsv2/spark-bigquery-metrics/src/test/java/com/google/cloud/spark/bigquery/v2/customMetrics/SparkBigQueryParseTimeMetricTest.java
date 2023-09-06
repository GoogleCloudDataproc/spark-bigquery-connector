package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.*;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class SparkBigQueryParseTimeMetricTest {
  private final SparkBigQueryParseTimeMetric sparkBigQueryParseTimeMetric =
      new SparkBigQueryParseTimeMetric();

  @Test
  public void testName() {
    assertThat(sparkBigQueryParseTimeMetric.name()).isEqualTo(BIG_QUERY_PARSE_TIME_METRIC_NAME);
  }

  @Test
  public void testDescription() {
    assertThat(sparkBigQueryParseTimeMetric.description())
        .isEqualTo(BIG_QUERY_PARSE_TIME_METRIC_DESCRIPTION);
  }

  @Test
  public void testAggregateMetrics() {
    assertThat(sparkBigQueryParseTimeMetric.aggregateTaskMetrics(new long[] {1020L, 2030L, 3040L}))
        .isEqualTo("total (min, med, max)\n" + "6.1 s (1.0 s, 2.0 s, 3.0 s)");
  }
}
