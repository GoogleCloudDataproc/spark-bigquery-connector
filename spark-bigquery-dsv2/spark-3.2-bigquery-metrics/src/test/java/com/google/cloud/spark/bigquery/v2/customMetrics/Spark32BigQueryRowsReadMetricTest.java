package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.Spark32BigQueryCustomMetricConstants.*;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class Spark32BigQueryRowsReadMetricTest {
  private final Spark32BigQueryRowsReadMetric spark32BigQueryRowsReadMetric =
      new Spark32BigQueryRowsReadMetric();

  @Test
  public void testName() {
    assertThat(spark32BigQueryRowsReadMetric.name()).isEqualTo(BIG_QUERY_ROWS_READ_METRIC_NAME);
  }

  @Test
  public void testDescription() {
    assertThat(spark32BigQueryRowsReadMetric.description())
        .isEqualTo(BIG_QUERY_ROWS_READ_METRIC_DESCRIPTION);
  }

  @Test
  public void testAggregateMetrics() {
    assertThat(spark32BigQueryRowsReadMetric.aggregateTaskMetrics(new long[] {1000L, 2000L}))
        .isEqualTo("3000");
  }
}
