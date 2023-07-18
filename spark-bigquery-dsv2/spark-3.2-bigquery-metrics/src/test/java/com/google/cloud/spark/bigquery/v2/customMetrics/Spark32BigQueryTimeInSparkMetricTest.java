package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.Spark32BigQueryCustomMetricConstants.*;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class Spark32BigQueryTimeInSparkMetricTest {
  private final Spark32BigQueryTimeInSparkMetric spark32BigQueryTimeInSparkMetric =
      new Spark32BigQueryTimeInSparkMetric();

  @Test
  public void testName() {
    assertThat(spark32BigQueryTimeInSparkMetric.name())
        .isEqualTo(BIG_QUERY_TIME_IN_SPARK_METRIC_NAME);
  }

  @Test
  public void testDescription() {
    assertThat(spark32BigQueryTimeInSparkMetric.description())
        .isEqualTo(BIG_QUERY_TIME_IN_SPARK_METRIC_DESCRIPTION);
  }

  @Test
  public void testAggregateMetrics() {
    assertThat(spark32BigQueryTimeInSparkMetric.aggregateTaskMetrics(new long[] {1000L, 2000L}))
        .isEqualTo("3000");
  }
}
