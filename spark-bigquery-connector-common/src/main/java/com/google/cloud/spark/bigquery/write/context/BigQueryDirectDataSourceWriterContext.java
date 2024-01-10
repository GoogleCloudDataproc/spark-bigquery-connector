/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.write.context;

import static com.google.cloud.spark.bigquery.ProtobufUtils.toProtoSchema;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.spark.bigquery.PartitionOverwriteMode;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDirectDataSourceWriterContext implements DataSourceWriterContext {

  final Logger logger = LoggerFactory.getLogger(BigQueryDirectDataSourceWriterContext.class);

  private final BigQueryClient bigQueryClient;
  private final BigQueryClientFactory writeClientFactory;
  private final TableId destinationTableId;
  private final StructType sparkSchema;
  private final ProtoSchema protoSchema;
  private final String writeUUID;
  private final RetrySettings bigqueryDataWriterHelperRetrySettings;
  private final Optional<String> traceId;
  private final boolean enableModeCheckForSchemaFields;
  private final ImmutableMap<String, String> tableLabels;

  private final BigQueryTable tableToWrite;
  private final String tablePathForBigQueryStorage;
  private final SchemaConvertersConfiguration schemaConvertersConfiguration;
  private final Optional<String> destinationTableKmsKeyName;
  private final boolean writeAtLeastOnce;

  private BigQueryWriteClient writeClient;
  private Optional<TableInfo> tableInfo = Optional.absent();

  private PartitionOverwriteMode overwriteMode;

  enum WritingMode {
    IGNORE_INPUTS,
    APPEND_AT_LEAST_ONCE,
    OVERWRITE,
    ALL_ELSE
  }

  private WritingMode writingMode = WritingMode.ALL_ELSE;

  public BigQueryDirectDataSourceWriterContext(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      TableId destinationTableId,
      String writeUUID,
      SaveMode saveMode,
      StructType sparkSchema,
      RetrySettings bigqueryDataWriterHelperRetrySettings,
      Optional<String> traceId,
      boolean enableModeCheckForSchemaFields,
      ImmutableMap<String, String> tableLabels,
      SchemaConvertersConfiguration schemaConvertersConfiguration,
      java.util.Optional<String> destinationTableKmsKeyName,
      boolean writeAtLeastOnce,
      PartitionOverwriteMode overwriteMode)
      throws IllegalArgumentException {
    this.bigQueryClient = bigQueryClient;
    this.writeClientFactory = bigQueryWriteClientFactory;
    this.destinationTableId = destinationTableId;
    this.writeUUID = writeUUID;
    this.sparkSchema = sparkSchema;
    this.bigqueryDataWriterHelperRetrySettings = bigqueryDataWriterHelperRetrySettings;
    this.traceId = traceId;
    this.enableModeCheckForSchemaFields = enableModeCheckForSchemaFields;
    this.tableLabels = tableLabels;
    this.schemaConvertersConfiguration = schemaConvertersConfiguration;
    this.destinationTableKmsKeyName = Optional.fromJavaUtil(destinationTableKmsKeyName);
    this.writeAtLeastOnce = writeAtLeastOnce;
    Schema bigQuerySchema =
        SchemaConverters.from(this.schemaConvertersConfiguration).toBigQuerySchema(sparkSchema);
    try {
      this.protoSchema = toProtoSchema(sparkSchema);
    } catch (IllegalArgumentException e) {
      throw new BigQueryConnectorException.InvalidSchemaException(
          "Could not convert Spark schema to protobuf descriptor", e);
    }

    this.tableToWrite = getOrCreateTable(saveMode, destinationTableId, bigQuerySchema);
    this.tablePathForBigQueryStorage =
        bigQueryClient.createTablePathForBigQueryStorage(tableToWrite.getTableId());

    if (!writingMode.equals(WritingMode.IGNORE_INPUTS)) {
      this.writeClient = writeClientFactory.getBigQueryWriteClient();
    }
    this.overwriteMode = overwriteMode;
  }

  /**
   * This function determines whether the destination table exists: if it doesn't, we will create a
   * table and Spark will directly write to it.
   *
   * @param saveMode the SaveMode supplied by the user.
   * @param destinationTableId the TableId, as was supplied by the user
   * @param bigQuerySchema the bigQuery schema
   * @return The TableId to which Spark will do the writing: whether that is the destinationTableID
   *     or the temporaryTableId.
   */
  private BigQueryTable getOrCreateTable(
      SaveMode saveMode, TableId destinationTableId, Schema bigQuerySchema)
      throws IllegalArgumentException {
    if (bigQueryClient.tableExists(destinationTableId)) {
      TableInfo destinationTable = bigQueryClient.getTable(destinationTableId);
      Schema tableSchema = destinationTable.getDefinition().getSchema();
      Preconditions.checkArgument(
          BigQueryUtil.schemaWritable(
              bigQuerySchema, // sourceSchema
              tableSchema, // destinationSchema
              false, // regardFieldOrder
              enableModeCheckForSchemaFields),
          new BigQueryConnectorException.InvalidSchemaException(
              "Destination table's schema is not compatible with dataframe's schema"));
      switch (saveMode) {
        case Append:
          if (writeAtLeastOnce) {
            writingMode = WritingMode.APPEND_AT_LEAST_ONCE;
            return new BigQueryTable(
                bigQueryClient.createTempTable(destinationTableId, tableSchema).getTableId(), true);
          }
          break;
        case Overwrite:
          writingMode = WritingMode.OVERWRITE;
          return new BigQueryTable(
              bigQueryClient.createTempTable(destinationTableId, tableSchema).getTableId(), true);
        case Ignore:
          writingMode = WritingMode.IGNORE_INPUTS;
          break;
        case ErrorIfExists:
          throw new IllegalArgumentException("Table already exists in BigQuery");
      }
      return new BigQueryTable(destinationTable.getTableId(), false);
    } else {
      return new BigQueryTable(
          bigQueryClient
              .createTable(
                  destinationTableId,
                  bigQuerySchema,
                  BigQueryClient.CreateTableOptions.of(
                      destinationTableKmsKeyName.toJavaUtil(), tableLabels))
              .getTableId(),
          true);
    }
  }

  @Override
  public DataWriterContextFactory<InternalRow> createWriterContextFactory() {
    return new BigQueryDirectDataWriterContextFactory(
        writeClientFactory,
        tablePathForBigQueryStorage,
        sparkSchema,
        protoSchema,
        writingMode.equals(WritingMode.IGNORE_INPUTS),
        bigqueryDataWriterHelperRetrySettings,
        traceId,
        writeAtLeastOnce);
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessageContext message) {}

  /**
   * This function will determine, based on the WritingMode: if in IGNORE_INPUTS mode, no work is to
   * be done; otherwise all streams will be batch committed using the BigQuery Storage Write API,
   * and then: if in OVERWRITE mode, the overwriteDestinationWithTemporary function from
   * BigQueryClient will be called to replace the destination table with all the data from the
   * temporary table; if in OVERWRITE mode with dynamic partitions enabled,
   * overwriteDestinationWithTemporaryDynamicPartitons from BigQueryClient will be called to replace
   * the required partitions;if in ALL_ELSE mode no more work needs to be done.
   *
   * @see WritingMode
   * @see BigQueryClient#overwriteDestinationWithTemporary(TableId temporaryTableId, TableId
   *     destinationTableId)
   * @param messages the BigQueryWriterCommitMessage array returned by the BigQueryDataWriter's.
   */
  @Override
  public void commit(WriterCommitMessageContext[] messages) {
    if (writingMode.equals(WritingMode.IGNORE_INPUTS)) return;
    logger.info(
        "BigQuery DataSource writer {} committed with messages:\n{}",
        writeUUID,
        Arrays.toString(messages));

    if (!writeAtLeastOnce) {
      BatchCommitWriteStreamsRequest.Builder batchCommitWriteStreamsRequest =
          BatchCommitWriteStreamsRequest.newBuilder().setParent(tablePathForBigQueryStorage);
      for (WriterCommitMessageContext message : messages) {
        batchCommitWriteStreamsRequest.addWriteStreams(
            ((BigQueryDirectWriterCommitMessageContext) message).getWriteStreamName());
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
    }

    if (writingMode.equals(WritingMode.APPEND_AT_LEAST_ONCE)
        || writingMode.equals(WritingMode.OVERWRITE)) {
      Job queryJob =
          (writingMode.equals(WritingMode.OVERWRITE))
              ? overwriteMode == PartitionOverwriteMode.STATIC
                  ? bigQueryClient.overwriteDestinationWithTemporary(
                      tableToWrite.getTableId(), destinationTableId)
                  : bigQueryClient.overwriteDestinationWithTemporaryDynamicPartitons(
                      tableToWrite.getTableId(), destinationTableId)
              : bigQueryClient.appendDestinationWithTemporary(
                  tableToWrite.getTableId(), destinationTableId);
      bigQueryClient.waitForJob(queryJob);
      Preconditions.checkState(
          bigQueryClient.deleteTable(tableToWrite.getTableId()),
          new BigQueryConnectorException(
              String.format("Could not delete temporary table %s from BigQuery", tableToWrite)));
    }

    if (!tableLabels.isEmpty()) {
      TableInfo originalTableInfo = bigQueryClient.getTable(destinationTableId);
      TableInfo.Builder updatedTableInfo = originalTableInfo.toBuilder();
      updatedTableInfo.setLabels(tableLabels);
      bigQueryClient.update(updatedTableInfo.build());
    }
  }

  /**
   * If not in WritingMode IGNORE_INPUTS, the BigQuery Storage Write API WriteClient is shut down.
   *
   * @see BigQueryWriteClient
   * @param messages the BigQueryWriterCommitMessage array returned by the BigQueryDataWriter's.
   */
  @Override
  public void abort(WriterCommitMessageContext[] messages) {
    logger.warn("BigQuery Data Source writer {} aborted", writeUUID);
    if (writingMode.equals(WritingMode.IGNORE_INPUTS)) return;

    // Deletes the preliminary table we wrote to (if it exists):
    if (tableToWrite.toDeleteOnAbort()) {
      bigQueryClient.deleteTable(tableToWrite.getTableId());
    }
  }

  @Override
  public void setTableInfo(TableInfo tableInfo) {
    this.tableInfo = Optional.fromNullable(tableInfo);
  }

  // Used for the getOrCreateTable output, to indicate if the table should be deleted on abort
  static class BigQueryTable {
    private final TableId tableId;
    private final boolean toDeleteOnAbort;

    public BigQueryTable(TableId tableId, boolean toDeleteOnAbort) {
      this.tableId = tableId;
      this.toDeleteOnAbort = toDeleteOnAbort;
    }

    public TableId getTableId() {
      return tableId;
    }

    public boolean toDeleteOnAbort() {
      return toDeleteOnAbort;
    }
  }
}
