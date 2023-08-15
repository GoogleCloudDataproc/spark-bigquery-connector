package com.google.cloud.spark.bigquery.v2.customMetrics;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public class SparkBigQueryTaskMetric implements CustomTaskMetric {
  private final String name;
  private final long value;

  public SparkBigQueryTaskMetric(String name, long value) {
    this.name = name;
    this.value = value;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public long value() {
    return value;
  }
}
