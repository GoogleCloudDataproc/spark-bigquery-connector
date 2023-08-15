package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.*;

import org.apache.spark.sql.connector.metric.CustomSumMetric;

public class SparkBigQueryRowsReadMetric extends CustomSumMetric {
  @Override
  public String name() {
    return BIG_QUERY_ROWS_READ_METRIC_NAME;
  }

  @Override
  public String description() {
    return BIG_QUERY_ROWS_READ_METRIC_DESCRIPTION;
  }
}
