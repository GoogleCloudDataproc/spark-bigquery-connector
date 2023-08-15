package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.*;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class SparkBigQueryTimeInSparkMetricTest {
  private final SparkBigQueryTimeInSparkMetric sparkBigQueryTimeInSparkMetric =
      new SparkBigQueryTimeInSparkMetric();

  @Test
  public void testName() {
    assertThat(sparkBigQueryTimeInSparkMetric.name())
        .isEqualTo(BIG_QUERY_TIME_IN_SPARK_METRIC_NAME);
  }

  @Test
  public void testDescription() {
    assertThat(sparkBigQueryTimeInSparkMetric.description())
        .isEqualTo(BIG_QUERY_TIME_IN_SPARK_METRIC_DESCRIPTION);
  }

  @Test
  public void testAggregateMetrics() {
    assertThat(sparkBigQueryTimeInSparkMetric.aggregateTaskMetrics(new long[] {1000L, 2000L}))
        .isEqualTo("3000");
  }
}
