package com.google.cloud.spark.bigquery.common;

import static com.google.cloud.spark.bigquery.ProtobufUtils.toProtoSchema;
import static com.google.cloud.spark.bigquery.SchemaConverters.toBigQuerySchema;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.storage.v1beta2.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1beta2.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1beta2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1beta2.ProtoSchema;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;

/** Helper class to enable direct write to bigquery from a Spark request */
public class GenericBigQueryDirectDataSourceWriter {
  private final BigQueryClient bigQueryClient;
  private final BigQueryClientFactory writeClientFactory;
  private final TableId destinationTableId;
  private final StructType sparkSchema;
  private final ProtoSchema protoSchema;
  private final String writeUUID;
  private final RetrySettings bigqueryDataWriterHelperRetrySettings;
  private BatchCommitWriteStreamsRequest.Builder batchCommitWriteStreamsRequest;

  private final TableId temporaryTableId;
  private final String tablePathForBigQueryStorage;

  private BigQueryWriteClient writeClient;

  enum WritingMode {
    IGNORE_INPUTS,
    OVERWRITE,
    ALL_ELSE
  }

  private WritingMode writingMode = WritingMode.ALL_ELSE;

  public GenericBigQueryDirectDataSourceWriter(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      TableId destinationTableId,
      String writeUUID,
      SaveMode saveMode,
      StructType sparkSchema,
      RetrySettings bigqueryDataWriterHelperRetrySettings)
      throws IllegalArgumentException {
    this.bigQueryClient = bigQueryClient;
    this.writeClientFactory = bigQueryWriteClientFactory;
    this.destinationTableId = destinationTableId;
    this.writeUUID = writeUUID;
    this.sparkSchema = sparkSchema;
    this.bigqueryDataWriterHelperRetrySettings = bigqueryDataWriterHelperRetrySettings;
    Schema bigQuerySchema = toBigQuerySchema(sparkSchema);
    try {
      this.protoSchema = toProtoSchema(sparkSchema);
    } catch (IllegalArgumentException e) {
      throw new BigQueryConnectorException.InvalidSchemaException(
          "Could not convert Spark schema to protobuf descriptor", e);
    }

    this.temporaryTableId = getOrCreateTable(saveMode, destinationTableId, bigQuerySchema);
    this.tablePathForBigQueryStorage =
        bigQueryClient.createTablePathForBigQueryStorage(temporaryTableId);

    if (!writingMode.equals(WritingMode.IGNORE_INPUTS)) {
      this.writeClient = writeClientFactory.createBigQueryWriteClient();
    }
  }

  /**
   * This function determines whether the destination table exists: if it doesn't, Spark will do the
   * writing directly to it; otherwise, if SaveMode = SaveMode.OVERWRITE then a temporary table will
   * be created, where the writing results will be stored before replacing the destination table
   * upon commit; this function also validates the destination table's schema matches the expected
   * schema, if applicable.
   *
   * @param saveMode the SaveMode supplied by the user.
   * @param destinationTableId the TableId, as was supplied by the user
   * @param bigQuerySchema the bigQuery schema
   * @return The TableId to which Spark will do the writing: whether that is the destinationTableID
   *     or the temporaryTableId.
   */
  private TableId getOrCreateTable(
      SaveMode saveMode, TableId destinationTableId, Schema bigQuerySchema)
      throws IllegalArgumentException {
    if (bigQueryClient.tableExists(destinationTableId)) {
      //      Preconditions.checkArgument(
      //          bigQueryClient
      //              .getTable(destinationTableId)
      //              .getDefinition()
      //              .getSchema()
      //              .getFields()
      //              .equals(bigQuerySchema.getFields()),
      //          new BigQueryConnectorException.InvalidSchemaException(
      //              "Destination table's schema is not compatible with dataframe's schema"));
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
          throw new IllegalArgumentException("Table already exists in BigQuery");
      }
      return bigQueryClient.getTable(destinationTableId).getTableId();
    } else {
      return bigQueryClient.createTable(destinationTableId, bigQuerySchema).getTableId();
    }
  }

  public boolean evaluateIfIgnoreInput() {
    if (writingMode.equals(WritingMode.IGNORE_INPUTS)) return true;
    return false;
  }

  /**
   * This function will determine, based on the WritingMode: if in IGNORE_INPUTS mode, no work is to
   * be done; otherwise all streams will be batch committed using the BigQuery Stora2ge Write API,
   * and then: if in OVERWRITE mode, the overwriteDestinationWithTemporary function from
   * BigQueryClient will be called to replace the destination table with all the data from the
   * temporary table; if in ALL_ELSE mode no more work needs to be done.
   *
   * @see WritingMode
   * @see BigQueryClient#overwriteDestinationWithTemporary(TableId temporaryTableId, TableId
   *     destinationTableId)
   */
  public void commit(List<String> streamNames, Logger logger) {
    if (writingMode.equals(WritingMode.IGNORE_INPUTS)) return;

    this.batchCommitWriteStreamsRequest =
        BatchCommitWriteStreamsRequest.newBuilder().setParent(tablePathForBigQueryStorage);
    for (String streamName : streamNames) {
      batchCommitWriteStreamsRequest.addWriteStreams(streamName);
    }
    BatchCommitWriteStreamsResponse batchCommitWriteStreamsResponse =
        writeClient.batchCommitWriteStreams(batchCommitWriteStreamsRequest.build());

    if (!batchCommitWriteStreamsResponse.hasCommitTime()) {
      throw new BigQueryConnectorException(
          "DataSource writer failed to batch commit its BigQuery write-streams");
    }

    logger.info(
        "BigQuery DataSource writer has committed at time: {}",
        batchCommitWriteStreamsResponse.getCommitTime());

    if (writingMode.equals(WritingMode.OVERWRITE)) {
      Job overwriteJob =
          bigQueryClient.overwriteDestinationWithTemporary(temporaryTableId, destinationTableId);
      BigQueryClient.waitForJob(overwriteJob);
      Preconditions.checkState(
          bigQueryClient.deleteTable(temporaryTableId),
          new BigQueryConnectorException(
              String.format(
                  "Could not delete temporary table %s from BigQuery", temporaryTableId)));
    }

    writeClient.shutdown();
  }

  /**
   * If not in WritingMode IGNORE_INPUTS, the BigQuery Storage Write API WriteClient is shut down.
   *
   * @see BigQueryWriteClient // * @param messages the BigQueryWriterCommitMessage array returned by
   *     the BigQueryDataWriter's.
   */
  public void abort(Logger logger) {
    logger.warn("BigQuery Data Source writer {} aborted", writeUUID);
    if (writingMode.equals(WritingMode.IGNORE_INPUTS)) return;
    if (writeClient != null && !writeClient.isShutdown()) {
      writeClient.shutdown();
    }
    // Deletes the preliminary table we wrote to (if it exists):
    if (bigQueryClient.tableExists(temporaryTableId)) {
      bigQueryClient.deleteTable(temporaryTableId);
    }
  }

  public BigQueryClient getBigQueryClient() {
    return bigQueryClient;
  }

  public BigQueryClientFactory getWriteClientFactory() {
    return writeClientFactory;
  }

  public TableId getDestinationTableId() {
    return destinationTableId;
  }

  public StructType getSparkSchema() {
    return sparkSchema;
  }

  public ProtoSchema getProtoSchema() {
    return protoSchema;
  }

  public String getWriteUUID() {
    return writeUUID;
  }

  public RetrySettings getBigqueryDataWriterHelperRetrySettings() {
    return bigqueryDataWriterHelperRetrySettings;
  }

  public BatchCommitWriteStreamsRequest.Builder getBatchCommitWriteStreamsRequest() {
    return batchCommitWriteStreamsRequest;
  }

  public TableId getTemporaryTableId() {
    return temporaryTableId;
  }

  public String getTablePathForBigQueryStorage() {
    return tablePathForBigQueryStorage;
  }

  public BigQueryWriteClient getWriteClient() {
    return writeClient;
  }

  public WritingMode getWritingMode() {
    return writingMode;
  }

  public boolean isIgnoreInputs() {
    return writingMode.equals(WritingMode.IGNORE_INPUTS);
  }
}
