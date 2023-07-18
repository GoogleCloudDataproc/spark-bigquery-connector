package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.Spark32BigQueryCustomMetricConstants.*;

import org.apache.spark.sql.connector.metric.CustomMetric;

public class Spark32BigQueryRowsReadMetric implements CustomMetric {
  @Override
  public String name() {
    return BIG_QUERY_ROWS_READ_METRIC_NAME;
  }

  @Override
  public String description() {
    return BIG_QUERY_ROWS_READ_METRIC_DESCRIPTION;
  }

  @Override
  public String aggregateTaskMetrics(long[] taskMetrics) {
    long sum = initialValue;
    for (long taskMetric : taskMetrics) {
      sum += taskMetric;
    }
    return String.valueOf(sum);
  }
}
