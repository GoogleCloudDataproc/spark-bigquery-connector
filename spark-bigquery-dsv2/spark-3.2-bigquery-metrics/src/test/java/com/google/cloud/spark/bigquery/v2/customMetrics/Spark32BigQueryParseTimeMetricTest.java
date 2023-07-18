package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.Spark32BigQueryCustomMetricConstants.*;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class Spark32BigQueryParseTimeMetricTest {
  private final Spark32BigQueryParseTimeMetric spark32BigQueryParseTimeMetric =
      new Spark32BigQueryParseTimeMetric();

  @Test
  public void testName() {
    assertThat(spark32BigQueryParseTimeMetric.name()).isEqualTo(BIG_QUERY_PARSE_TIME_METRIC_NAME);
  }

  @Test
  public void testDescription() {
    assertThat(spark32BigQueryParseTimeMetric.description())
        .isEqualTo(BIG_QUERY_PARSE_TIME_METRIC_DESCRIPTION);
  }

  @Test
  public void testAggregateMetrics() {
    assertThat(spark32BigQueryParseTimeMetric.aggregateTaskMetrics(new long[] {1000L, 2000L}))
        .isEqualTo("3000");
  }
}
