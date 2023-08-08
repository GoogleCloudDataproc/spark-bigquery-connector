package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class SparkBigQueryTaskMetricTest {

  private final String testName = "test_name";
  private final long testValue = 1000L;
  private final SparkBigQueryTaskMetric sparkBigQueryTaskMetric =
      new SparkBigQueryTaskMetric(testName, testValue);

  @Test
  public void testName() {
    assertThat(sparkBigQueryTaskMetric.name()).isEqualTo(testName);
  }

  @Test
  public void testValue() {
    assertThat(sparkBigQueryTaskMetric.value()).isEqualTo(testValue);
  }
}
