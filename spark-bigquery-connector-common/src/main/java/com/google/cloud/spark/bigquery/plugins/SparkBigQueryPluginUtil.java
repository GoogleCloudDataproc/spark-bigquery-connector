package com.google.cloud.spark.bigquery.plugins;

import com.codahale.metrics.Counter;

public class SparkBigQueryPluginUtil {

  public static Counter parseTimeCounter = new Counter();
  public static Counter bytesReadCounter = new Counter();
  public static Counter rowsReadCounter = new Counter();
  public static Counter scanTimeCounter = new Counter();
  public static final String scanTime = "scanTime";
  public static final String parseTime = "parseTime";
  public static final String bytesRead = "bytesRead";
  public static final String rowsRead = "rowsRead";
  public static final String SUCCESS_MESSAGE = "SUCCESS";
  public static final String FAILED_MESSAGE = "FAILED";
}
