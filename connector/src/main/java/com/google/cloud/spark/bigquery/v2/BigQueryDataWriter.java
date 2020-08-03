package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1alpha2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.cloud.spark.bigquery.ProtobufUtils.buildSingleRowMessage;
import static com.google.cloud.spark.bigquery.ProtobufUtils.toDescriptor;

public class BigQueryDataWriter implements DataWriter<InternalRow> {

  final Logger logger = LoggerFactory.getLogger(BigQueryDataWriter.class);

  private final int partitionId;
  private final long taskId;
  private final long epochId;
  private final String tablePath;
  private final StructType sparkSchema;
  private final Descriptors.Descriptor schemaDescriptor;
  private final ProtoBufProto.ProtoSchema protoSchema;
  private final boolean ignoreInputs;

  private BigQueryDataWriterHelper writerHelper;

  public BigQueryDataWriter(
      int partitionId,
      long taskId,
      long epochId,
      BigQueryWriteClientFactory writeClientFactory,
      String tablePath,
      StructType sparkSchema,
      ProtoBufProto.ProtoSchema protoSchema,
      boolean ignoreInputs) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.epochId = epochId;
    this.tablePath = tablePath;
    this.sparkSchema = sparkSchema;
    try {
      this.schemaDescriptor = toDescriptor(sparkSchema);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException("Could not convert spark-schema to descriptor object.", e);
    }
    this.protoSchema = protoSchema;
    this.ignoreInputs = ignoreInputs;

    if (ignoreInputs) return;

    this.writerHelper =
        new BigQueryDataWriterHelper(writeClientFactory, tablePath, protoSchema, partitionId);
  }

  @Override
  public void write(InternalRow record) throws IOException {
    if (ignoreInputs) return;

    // if(partitionId == 3) abort(); FIXME: for debugging purposes.

    ByteString message =
        buildSingleRowMessage(sparkSchema, schemaDescriptor, record).toByteString();
    writerHelper.addRow(message);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    logger.debug("Data Writer {} commit()", partitionId);

    Long finalizedRowCount = null;
    String writeStreamName = null;

    if (!ignoreInputs) {
      writerHelper.finalizeStream();

      finalizedRowCount = writerHelper.getDataWriterRows();
      writeStreamName = writerHelper.getWriteStreamName();

      logger.debug(
          "Data Writer {}'s write-stream has finalized with row count: {}",
          partitionId,
          finalizedRowCount);
    }

    return new BigQueryWriterCommitMessage(
        writeStreamName, partitionId, taskId, epochId, tablePath, finalizedRowCount);
  }

  @Override
  public void abort() throws IOException {
    logger.debug("Data Writer {} abort()", partitionId);
    writerHelper.abort();
  }
}
