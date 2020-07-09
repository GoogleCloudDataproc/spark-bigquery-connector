package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1alpha2.Stream;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.util.Optional;

public class BigQueryWriterCommitMessage implements WriterCommitMessage {

    private final Optional<String> writeStreamName;
    private final int partitionId;
    private final long taskId;
    private final long epochId;
    private final TableId tableId;
    private final Optional<Long> rowCount;

    public BigQueryWriterCommitMessage(Optional<String> writeStreamName, int partitionId, long taskId, long epochId,
                                       TableId tableId, Optional<Long> rowCount) {
        this.writeStreamName = writeStreamName;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.tableId = tableId;
        this.rowCount = rowCount;
    }

    public Optional<String> getWriteStreamName() {
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

    public TableId getTableId() {
        return tableId;
    }

    public Optional<Long> getRowCount() {
        return rowCount;
    }

    @Override
    public String toString() {
        if(writeStreamName.isPresent() && rowCount.isPresent()) {
            return "BigQueryWriterCommitMessage{" +
                    ", writeStreamName='" + writeStreamName +
                    "partitionId=" + partitionId +
                    ", taskId=" + taskId +
                    ", epochId=" + epochId +
                    ", tableId='" + tableId +
                    ", rowCount='" + rowCount + '\'' +
                    '}';
        }
        return "BigQueryWriterCommitMessage{" +
                "partitionId=" + partitionId +
                ", taskId=" + taskId +
                ", epochId=" + epochId +
                ", tableId='" + tableId + '\'' +
                '}';
    }
}