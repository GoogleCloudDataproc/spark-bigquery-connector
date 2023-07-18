package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.Spark32BigQueryCustomMetricConstants.BIG_QUERY_SCAN_TIME_METRIC_DESCRIPTION;
import static com.google.cloud.spark.bigquery.v2.customMetrics.Spark32BigQueryCustomMetricConstants.BIG_QUERY_SCAN_TIME_METRIC_NAME;

import org.apache.spark.sql.connector.metric.CustomMetric;

public class Spark32BigQueryScanTimeMetric implements CustomMetric {

  @Override
  public String name() {
    return BIG_QUERY_SCAN_TIME_METRIC_NAME;
  }

  @Override
  public String description() {
    return BIG_QUERY_SCAN_TIME_METRIC_DESCRIPTION;
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
