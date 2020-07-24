package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1alpha2.Storage;
import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.google.cloud.spark.bigquery.ProtobufUtils.toDescriptor;
import static com.google.cloud.spark.bigquery.SchemaConverters.toBigQuerySchema;

public class BigQueryDataSourceWriter implements DataSourceWriter {

  final Logger logger = LoggerFactory.getLogger(BigQueryDataSourceWriter.class);

  private final BigQueryClient bigQueryClient;
  private final BigQueryWriteClientFactory writeClientFactory;
  private final TableId destinationTableId;
  private final StructType sparkSchema;
  private final Schema bigQuerySchema;
  private final Descriptors.Descriptor schemaDescriptor;
  private final ProtoBufProto.ProtoSchema protoSchema;
  private final SaveMode saveMode;
  private final String writeUUID;

  private final TableId temporaryTableId;
  private final String tablePathForBigQueryStorage;

  private BigQueryWriteClient writeClient;

  enum WritingMode {
    IGNORE_INPUTS,
    OVERWRITE,
    ALL_ELSE
  }

  private WritingMode writingMode = WritingMode.ALL_ELSE;

  public BigQueryDataSourceWriter(
      BigQueryClient bigQueryClient,
      BigQueryWriteClientFactory bigQueryWriteClientFactory,
      TableId destinationTableId,
      String writeUUID,
      SaveMode saveMode,
      StructType sparkSchema) {
    this.bigQueryClient = bigQueryClient;
    this.writeClientFactory = bigQueryWriteClientFactory;
    this.destinationTableId = destinationTableId;
    this.writeUUID = writeUUID;
    this.saveMode = saveMode;
    this.sparkSchema = sparkSchema;
    this.bigQuerySchema = toBigQuerySchema(sparkSchema);
    try {
      this.schemaDescriptor = toDescriptor(sparkSchema);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException("Could not convert Spark schema to protobuf descriptor.", e);
    }
    this.protoSchema = ProtoSchemaConverter.convert(schemaDescriptor);

    this.temporaryTableId = getOrCreateTable(saveMode, destinationTableId, bigQuerySchema);
    this.tablePathForBigQueryStorage =
        bigQueryClient.createTablePathForBigQueryStorage(temporaryTableId);

    if (!writingMode.equals(WritingMode.IGNORE_INPUTS)) {
      this.writeClient = writeClientFactory.createBigQueryWriteClient();
    }
  }

  private TableId getOrCreateTable(
      SaveMode saveMode, TableId destinationTableId, Schema bigQuerySchema) {
    if (bigQueryClient.tableExists(destinationTableId)) {
      switch (saveMode) {
        case Append:
          break;
        case Overwrite:
          writingMode = WritingMode.OVERWRITE;
          return bigQueryClient.createTempTable(destinationTableId, bigQuerySchema).getTableId();
        case Ignore:
          writingMode = WritingMode.IGNORE_INPUTS;
          break;
        case ErrorIfExists:
          throw new RuntimeException(
              "Table already exists in BigQuery."); // TODO: should this be a RuntimeException?
      }
      Preconditions.checkArgument(
          bigQueryClient
              .getTable(destinationTableId)
              .getDefinition()
              .getSchema()
              .equals(bigQuerySchema),
          new RuntimeException("Destination table's schema is not compatible."));
      return bigQueryClient.getTable(destinationTableId).getTableId();
    } else {
      return bigQueryClient.createTable(destinationTableId, bigQuerySchema).getTableId();
    }
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new BigQueryDataWriterFactory(
        writeClientFactory,
        tablePathForBigQueryStorage,
        sparkSchema,
        protoSchema,
        writingMode.equals(WritingMode.IGNORE_INPUTS));
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {}

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (writingMode.equals(WritingMode.IGNORE_INPUTS)) return;
    logger.info(
        "BigQuery DataSource writer {} committed with messages:\n{}",
        writeUUID,
        Arrays.toString(messages));

    Storage.BatchCommitWriteStreamsRequest.Builder batchCommitWriteStreamsRequest =
        Storage.BatchCommitWriteStreamsRequest.newBuilder().setParent(tablePathForBigQueryStorage);
    for (WriterCommitMessage message : messages) {
      batchCommitWriteStreamsRequest.addWriteStreams(
          ((BigQueryWriterCommitMessage) message).getWriteStreamName());
    }
    Storage.BatchCommitWriteStreamsResponse batchCommitWriteStreamsResponse =
        writeClient.batchCommitWriteStreams(batchCommitWriteStreamsRequest.build());

    if (!batchCommitWriteStreamsResponse.hasCommitTime()) {
      throw new RuntimeException(
          "DataSource writer failed to batch commit its BigQuery write-streams.");
    }

    logger.info(
        "BigQuery DataSource writer has committed at time: {}.",
        batchCommitWriteStreamsResponse.getCommitTime());

    if (writingMode.equals(WritingMode.OVERWRITE)) {
      bigQueryClient.overwriteDestinationWithTemporary(temporaryTableId, destinationTableId);
    }

    writeClient.shutdown();
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    logger.warn("BigQuery Data Source writer {} aborted.", writeUUID);
    if (writingMode.equals(WritingMode.IGNORE_INPUTS)) return;
    if (writeClient != null && !writeClient.isShutdown()) {
      writeClient.shutdown();
    }
  }
}
