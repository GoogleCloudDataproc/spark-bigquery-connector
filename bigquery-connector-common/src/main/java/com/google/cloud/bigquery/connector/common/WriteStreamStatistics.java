package com.google.cloud.bigquery.connector.common;

public class WriteStreamStatistics {
    private final long rowCount;
    private final long bytesWritten;

    public WriteStreamStatistics(long rowCount, long bytesWritten) {
        this.rowCount = rowCount;
        this.bytesWritten = bytesWritten;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }
}
