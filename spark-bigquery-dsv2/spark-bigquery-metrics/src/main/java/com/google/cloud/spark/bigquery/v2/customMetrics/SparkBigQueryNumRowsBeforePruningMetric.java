package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_NUM_ROWS_BEFORE_PRUNING_METRIC_DESCRIPTION;
import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_NUM_ROWS_BEFORE_PRUNING_METRIC_NAME;
import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_RAW_INPUT_ROWS_METRIC_DESCRIPTION;
import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.BIG_QUERY_RAW_INPUT_ROWS_METRIC_NAME;

import org.apache.spark.sql.connector.metric.CustomMetric;

public class SparkBigQueryNumRowsBeforePruningMetric implements CustomMetric {

  private final String name;
  private final String description;

  public SparkBigQueryNumRowsBeforePruningMetric() {
    this(
        BIG_QUERY_NUM_ROWS_BEFORE_PRUNING_METRIC_NAME,
        BIG_QUERY_NUM_ROWS_BEFORE_PRUNING_METRIC_DESCRIPTION);
  }

  private SparkBigQueryNumRowsBeforePruningMetric(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public static SparkBigQueryNumRowsBeforePruningMetric rawInputRowsAlias() {
    return new SparkBigQueryNumRowsBeforePruningMetric(
        BIG_QUERY_RAW_INPUT_ROWS_METRIC_NAME, BIG_QUERY_RAW_INPUT_ROWS_METRIC_DESCRIPTION);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public String aggregateTaskMetrics(long[] taskMetrics) {
    return MetricUtils.formatSumMetrics(taskMetrics);
  }
}
