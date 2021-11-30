package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class BigQueryDirectWriterCommitMessage implements WriterCommitMessage {

  private static final long serialVersionUID = -1562914502592461805L;
  private final String writeStreamName;
  private final int partitionId;
  private final long taskId;
  private final String tablePath;
  private final long rowCount;

  public BigQueryDirectWriterCommitMessage(
      String writeStreamName /*List<String> writeStreamNames*/,
      int partitionId,
      long taskId,
      String tablePath,
      long rowCount) {
    this.writeStreamName = writeStreamName;
    this.partitionId = partitionId;
    this.taskId = taskId;
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
        + ", tableId='"
        + tablePath
        + '\''
        + '}';
  }
}
