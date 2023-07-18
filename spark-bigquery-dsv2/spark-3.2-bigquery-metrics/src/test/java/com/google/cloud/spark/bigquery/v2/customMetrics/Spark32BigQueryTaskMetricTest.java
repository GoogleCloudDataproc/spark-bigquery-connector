package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class Spark32BigQueryTaskMetricTest {

  private final String testName = "test_name";
  private final long testValue = 1000L;
  private final Spark32BigQueryTaskMetric spark32BigQueryTaskMetric =
      new Spark32BigQueryTaskMetric(testName, testValue);

  @Test
  public void testName() {
    assertThat(spark32BigQueryTaskMetric.name()).isEqualTo(testName);
  }

  @Test
  public void testValue() {
    assertThat(spark32BigQueryTaskMetric.value()).isEqualTo(testValue);
  }
}
