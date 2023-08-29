package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_PARSE_TIME_METRIC_DESCRIPTION;
import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_PARSE_TIME_METRIC_NAME;

import org.apache.spark.sql.connector.metric.CustomMetric;

public class SparkBigQueryParseTimeMetric implements CustomMetric {

  @Override
  public String name() {
    return BIG_QUERY_PARSE_TIME_METRIC_NAME;
  }

  @Override
  public String description() {
    return BIG_QUERY_PARSE_TIME_METRIC_DESCRIPTION;
  }

  @Override
  public String aggregateTaskMetrics(long[] taskMetrics) {
    return MetricUtils.formatTimeMetrics(taskMetrics);
  }
}
