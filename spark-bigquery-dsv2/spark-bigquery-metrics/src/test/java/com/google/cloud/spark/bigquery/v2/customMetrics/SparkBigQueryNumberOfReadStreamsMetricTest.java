package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_NUMBER_OF_READ_STREAMS_METRIC_DESCRIPTION;
import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_NUMBER_OF_READ_STREAMS_METRIC_NAME;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class SparkBigQueryNumberOfReadStreamsMetricTest {
  private final SparkBigQueryNumberOfReadStreamsMetric sparkBigQueryNumberOfReadStreamsMetric =
      new SparkBigQueryNumberOfReadStreamsMetric();

  @Test
  public void testName() {
    assertThat(sparkBigQueryNumberOfReadStreamsMetric.name())
        .isEqualTo(BIG_QUERY_NUMBER_OF_READ_STREAMS_METRIC_NAME);
  }

  @Test
  public void testDescription() {
    assertThat(sparkBigQueryNumberOfReadStreamsMetric.description())
        .isEqualTo(BIG_QUERY_NUMBER_OF_READ_STREAMS_METRIC_DESCRIPTION);
  }

  @Test
  public void testAggregateMetrics() {
    assertThat(
            sparkBigQueryNumberOfReadStreamsMetric.aggregateTaskMetrics(new long[] {2L, 5L, 100L}))
        .isEqualTo("107");
  }
}
