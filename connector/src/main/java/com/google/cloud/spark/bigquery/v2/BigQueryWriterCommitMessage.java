package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

public class BigQueryWriterCommitMessage implements WriterCommitMessage {

    private final String writeStreamName;
    private final int partitionId;
    private final long taskId;
    private final long epochId;
    private final String tablePath;
    private final long rowCount;

    public BigQueryWriterCommitMessage(
            String writeStreamName /*List<String> writeStreamNames*/,
            int partitionId,
            long taskId,
            long epochId,
            String tablePath,
            long rowCount) {
        this.writeStreamName = writeStreamName;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.tablePath = tablePath;
        this.rowCount = rowCount;
    }

    public String getWriteStreamName() {
        return writeStreamName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getTaskId() {
        return taskId;
    }

    public long getEpochId() {
        return epochId;
    }

    public String getTablePath() {
        return tablePath;
    }

    public long getRowCount() {
        return rowCount;
    }

    @Override
    public String toString() {
        return "BigQueryWriterCommitMessage{"
                + "partitionId="
                + partitionId
                + ", taskId="
                + taskId
                + ", epochId="
                + epochId
                + ", tableId='"
                + tablePath
                + '\''
                + '}';
    }
}