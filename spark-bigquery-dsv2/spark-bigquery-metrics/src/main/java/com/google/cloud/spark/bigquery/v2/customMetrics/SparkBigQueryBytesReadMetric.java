package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_BYTES_READ_METRIC_DESCRIPTION;
import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_BYTES_READ_METRIC_NAME;

import org.apache.spark.sql.connector.metric.CustomSumMetric;

public class SparkBigQueryBytesReadMetric extends CustomSumMetric {

  @Override
  public String name() {
    return BIG_QUERY_BYTES_READ_METRIC_NAME;
  }

  @Override
  public String description() {
    return BIG_QUERY_BYTES_READ_METRIC_DESCRIPTION;
  }
}
