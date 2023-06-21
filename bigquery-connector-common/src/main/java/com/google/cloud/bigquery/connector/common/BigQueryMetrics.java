package com.google.cloud.bigquery.connector.common;

public interface BigQueryMetrics {
  void incrementBytesReadCounter(long val);

  void incrementRowsReadCounter(long val);

  void updateScanTime(long val);

  void updateParseTime(long val);

  void updateTimeInSpark(long val);
}
