package com.google.cloud.spark.bigquery.v2;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.storage.v1beta2.ProtoSchema;
import com.google.cloud.spark.bigquery.common.BigQueryDirectDataWriterHelper;
import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDirectDataWriter implements DataWriter<InternalRow> {
  final Logger logger = LoggerFactory.getLogger(BigQueryDirectDataWriter.class);

  private final int partitionId;
  private final long taskId;

  private BigQueryDirectDataWriterHelper writerHelper;

  public BigQueryDirectDataWriter(
      int partitionId,
      long taskId,
      BigQueryClientFactory writeClientFactory,
      String tablePath,
      StructType sparkSchema,
      ProtoSchema protoSchema,
      RetrySettings bigqueryDataWriterHelperRetrySettings) {
    this.partitionId = partitionId;
    this.taskId = taskId;

    this.writerHelper =
        new BigQueryDirectDataWriterHelper(
            writeClientFactory,
            tablePath,
            sparkSchema,
            protoSchema,
            bigqueryDataWriterHelperRetrySettings);
  }

  @Override
  public void write(InternalRow internalRow) throws IOException {
    writerHelper.addRow(internalRow);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    logger.debug("Data Writer {} commit()", partitionId);

    long rowCount = writerHelper.commit();
    String writeStreamName = writerHelper.getWriteStreamName();

    logger.debug(
        "Data Writer {}'s write-stream has finalized with row count: {}", partitionId, rowCount);

    return new BigQueryDirectWriterCommitMessage(
        writeStreamName, partitionId, taskId, writerHelper.getTablePath(), rowCount);
  }

  @Override
  public void abort() throws IOException {
    logger.debug("Data Writer {} abort()", partitionId);
    writerHelper.abort();
  }

  @Override
  public void close() throws IOException {}
}
