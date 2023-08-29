package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_ROWS_READ_METRIC_DESCRIPTION;
import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_ROWS_READ_METRIC_NAME;

import org.apache.spark.sql.connector.metric.CustomMetric;

public class SparkBigQueryRowsReadMetric implements CustomMetric {
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
    return MetricUtils.formatSumMetrics(taskMetrics);
  }
}
