package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.util.List;

public class BigQueryWriterCommitMessage implements WriterCommitMessage {

    private final List<String> writeStreamNames;
    private final int partitionId;
    private final long taskId;
    private final long epochId;
    private final String tablePath;
    private final Long rowCount;

    public BigQueryWriterCommitMessage(/*String writeStreamName*/List<String> writeStreamNames, int partitionId, long taskId, long epochId,
                                                                 String tablePath, Long rowCount) {
        this.writeStreamNames = writeStreamNames;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.tablePath = tablePath;
        this.rowCount = rowCount;
    }

    public List<String> getWriteStreamNames() throws NoSuchFieldError{
        if(writeStreamNames == null) {
            throw new NoSuchFieldError("Data writer did not create a write-stream.");
        }
        return writeStreamNames;
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

    public long getRowCount() throws NoSuchFieldError{
        if(rowCount == null) {
            throw new NoSuchFieldError("Data write did not create a write-stream and did not write to BigQuery.");
        }
        return rowCount;
    }

    @Override
    public String toString() {
        if(writeStreamNames != null && rowCount != null) {
            return "BigQueryWriterCommitMessage{" +
                    ", writeStreamName='" + writeStreamNames +
                    "partitionId=" + partitionId +
                    ", taskId=" + taskId +
                    ", epochId=" + epochId +
                    ", tableId='" + tablePath +
                    ", rowCount='" + rowCount + '\'' +
                    '}';
        }
        return "BigQueryWriterCommitMessage{" +
                "partitionId=" + partitionId +
                ", taskId=" + taskId +
                ", epochId=" + epochId +
                ", tableId='" + tablePath + '\'' +
                '}';
    }
}