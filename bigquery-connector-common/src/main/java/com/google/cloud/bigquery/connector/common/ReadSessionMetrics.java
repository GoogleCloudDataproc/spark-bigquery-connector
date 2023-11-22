package com.google.cloud.bigquery.connector.common;

public interface ReadSessionMetrics {
    public void incrementBytesReadAccumulator(long value);
    public void incrementRowsReadAccumulator(long value);
    public void incrementScanTimeAccumulator(long value);
    public void incrementParseTimeAccumulator(long value);
}
