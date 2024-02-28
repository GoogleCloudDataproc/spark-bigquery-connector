package com.google.cloud.spark.bigquery.v2.customMetrics;

public class SparkBigQueryCustomMetricConstants {
  public static final String BIG_QUERY_BYTES_READ_METRIC_NAME = "bqBytesRead";
  static final String BIG_QUERY_BYTES_READ_METRIC_DESCRIPTION = "number of BQ bytes read";
  public static final String BIG_QUERY_ROWS_READ_METRIC_NAME = "bqRowsRead";
  static final String BIG_QUERY_ROWS_READ_METRIC_DESCRIPTION = "number of BQ rows read";
  public static final String BIG_QUERY_SCAN_TIME_METRIC_NAME = "bqScanTime";
  static final String BIG_QUERY_SCAN_TIME_METRIC_DESCRIPTION = "scan time for BQ";
  public static final String BIG_QUERY_PARSE_TIME_METRIC_NAME = "bqParseTime";
  static final String BIG_QUERY_PARSE_TIME_METRIC_DESCRIPTION = "parsing time for BQ";
  public static final String BIG_QUERY_TIME_IN_SPARK_METRIC_NAME = "bqTimeInSpark";
  static final String BIG_QUERY_TIME_IN_SPARK_METRIC_DESCRIPTION = "time spent in spark";
  public static final String BIG_QUERY_NUMBER_OF_READ_STREAMS_METRIC_NAME = "bqNumReadStreams";
  static final String BIG_QUERY_NUMBER_OF_READ_STREAMS_METRIC_DESCRIPTION =
      "number of read streams";
}
