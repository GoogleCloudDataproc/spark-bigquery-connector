package com.google.cloud.bigquery.connector.common;

public interface ReadSessionMetrics {
  void incrementBytesReadAccumulator(long value);

  void incrementRowsReadAccumulator(long value);

  void incrementScanTimeAccumulator(long value);

  void incrementParseTimeAccumulator(long value);
}
