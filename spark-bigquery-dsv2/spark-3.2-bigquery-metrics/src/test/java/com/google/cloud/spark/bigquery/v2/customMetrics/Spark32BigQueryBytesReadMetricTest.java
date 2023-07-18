package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.Spark32BigQueryCustomMetricConstants.BIG_QUERY_BYTES_READ_METRIC_DESCRIPTION;
import static com.google.cloud.spark.bigquery.v2.customMetrics.Spark32BigQueryCustomMetricConstants.BIG_QUERY_BYTES_READ_METRIC_NAME;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class Spark32BigQueryBytesReadMetricTest {
  private final Spark32BigQueryBytesReadMetric spark32BigQueryBytesReadMetric =
      new Spark32BigQueryBytesReadMetric();

  @Test
  public void testName() {
    assertThat(spark32BigQueryBytesReadMetric.name()).isEqualTo(BIG_QUERY_BYTES_READ_METRIC_NAME);
  }

  @Test
  public void testDescription() {
    assertThat(spark32BigQueryBytesReadMetric.description())
        .isEqualTo(BIG_QUERY_BYTES_READ_METRIC_DESCRIPTION);
  }

  @Test
  public void testAggregateMetrics() {
    assertThat(spark32BigQueryBytesReadMetric.aggregateTaskMetrics(new long[] {1000L, 2000L}))
        .isEqualTo("3000");
  }
}
