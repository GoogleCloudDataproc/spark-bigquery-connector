package com.google.cloud.bigquery.connector.common;

public interface BigQueryMetrics {
  void incrementBytesReadCounter(long val);

  void incrementRowsReadCounter(long val);

  void incrementScanTimeCounter(long val);
}
