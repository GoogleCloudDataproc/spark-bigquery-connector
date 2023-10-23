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

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.AvroSchemaConverter;
import com.google.cloud.spark.bigquery.PartitionOverwriteMode;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryUtil;
import com.google.cloud.spark.bigquery.write.BigQueryWriteHelper;
import com.google.cloud.spark.bigquery.write.IntermediateDataCleaner;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DataSourceWriter implemented by first writing the DataFrame's data into GCS in an intermediate
 * format, and then triggering a BigQuery load job on this data. Hence the "indirect" - the data
 * goes through an intermediate storage.
 */
public class BigQueryIndirectDataSourceWriterContext implements DataSourceWriterContext {

  private static final Logger logger =
      LoggerFactory.getLogger(BigQueryIndirectDataSourceWriterContext.class);

  private final BigQueryClient bigQueryClient;
  private final SparkBigQueryConfig config;
  private final Configuration hadoopConfiguration;
  private final StructType sparkSchema;
  private final String writeUUID;
  private final SaveMode saveMode;
  private final Path gcsPath;
  private final Optional<IntermediateDataCleaner> intermediateDataCleaner;

  private Optional<TableInfo> tableInfo = Optional.empty();
  private final Schema tableSchema;
  private final JobInfo.WriteDisposition writeDisposition;

  private TableId temporaryTableId = null;

  public BigQueryIndirectDataSourceWriterContext(
      BigQueryClient bigQueryClient,
      SparkBigQueryConfig config,
      Configuration hadoopConfiguration,
      StructType sparkSchema,
      String writeUUID,
      SaveMode saveMode,
      Path gcsPath,
      Optional<IntermediateDataCleaner> intermediateDataCleaner) {
    this.bigQueryClient = bigQueryClient;
    this.config = config;
    this.hadoopConfiguration = hadoopConfiguration;
    this.sparkSchema = sparkSchema;
    this.writeUUID = writeUUID;
    this.saveMode = saveMode;
    this.gcsPath = gcsPath;
    this.intermediateDataCleaner = intermediateDataCleaner;

    Schema schema =
        SchemaConverters.from(SchemaConvertersConfiguration.from(config))
            .toBigQuerySchema(sparkSchema);
    if (tableInfo.isPresent()) {
      schema =
          BigQueryUtil.adjustSchemaIfNeeded(schema, tableInfo.get().getDefinition().getSchema());
    }
    this.tableSchema = schema;
    this.writeDisposition = SparkBigQueryUtil.saveModeToWriteDisposition(saveMode);
  }

  @Override
  public DataWriterContextFactory<InternalRow> createWriterContextFactory() {
    org.apache.avro.Schema avroSchema = AvroSchemaConverter.sparkSchemaToAvroSchema(sparkSchema);
    return new BigQueryIndirectDataWriterContextFactory(
        new SerializableConfiguration(hadoopConfiguration),
        gcsPath.toString(),
        sparkSchema,
        avroSchema.toString());
  }

  @Override
  public void commit(WriterCommitMessageContext[] messages) {
    logger.info(
        "Data has been successfully written to GCS. Going to load {} files to BigQuery",
        messages.length);
    try {
      List<String> sourceUris =
          Stream.of(messages)
              .map(msg -> ((BigQueryIndirectWriterCommitMessageContext) msg).getUri())
              .collect(Collectors.toList());

      JobInfo.WriteDisposition writeDisposition =
          SparkBigQueryUtil.saveModeToWriteDisposition(saveMode);
      if (writeDisposition == JobInfo.WriteDisposition.WRITE_TRUNCATE
          && config.getPartitionOverwriteModeValue() == PartitionOverwriteMode.DYNAMIC
          && bigQueryClient.tableExists(config.getTableId())) {
        temporaryTableId = createTempTable(config.getTableId(), tableSchema);
        loadDataToBigQuery(sourceUris);
        Job queryJob =
            bigQueryClient.overwriteDestinationWithTemporaryDynamicPartitons(
                temporaryTableId, config.getTableId());
        BigQueryClient.waitForJob(queryJob);
        Preconditions.checkState(
            bigQueryClient.deleteTable(temporaryTableId),
            new BigQueryConnectorException(
                String.format(
                    "Could not delete temporary table %s from BigQuery", temporaryTableId)));
      } else {
        loadDataToBigQuery(sourceUris);
      }
      updateMetadataIfNeeded();
      logger.info("Data has been successfully loaded to BigQuery");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      cleanTemporaryGcsPathIfNeeded();
    }
  }

  @Override
  public void abort(WriterCommitMessageContext[] messages) {
    try {
      logger.warn(
          "Aborting write {} for table {}",
          writeUUID,
          BigQueryUtil.friendlyTableName(config.getTableId()));
    } finally {
      cleanTemporaryGcsPathIfNeeded();
    }
  }

  @Override
  public void setTableInfo(TableInfo tableInfo) {
    this.tableInfo = Optional.ofNullable(tableInfo);
  }

  private TableId createTempTable(TableId destinationTableId, Schema bigQuerySchema)
      throws IllegalArgumentException {
    TableInfo destinationTable = bigQueryClient.getTable(destinationTableId);
    Schema tableSchema = destinationTable.getDefinition().getSchema();
    Preconditions.checkArgument(
        BigQueryUtil.schemaWritable(
            bigQuerySchema, // sourceSchema
            tableSchema, // destinationSchema
            false, // regardFieldOrder
            config.getEnableModeCheckForSchemaFields()),
        new BigQueryConnectorException.InvalidSchemaException(
            "Destination table's schema is not compatible with dataframe's schema"));
    return bigQueryClient.createTempTable(destinationTableId, bigQuerySchema).getTableId();
  }

  void loadDataToBigQuery(List<String> sourceUris) throws IOException {
    // Solving Issue #248
    List<String> optimizedSourceUris = SparkBigQueryUtil.optimizeLoadUriListForSpark(sourceUris);

    if (temporaryTableId != null) {
      bigQueryClient.loadDataIntoTable(
          config,
          optimizedSourceUris,
          FormatOptions.avro(),
          writeDisposition,
          Optional.of(tableSchema),
          temporaryTableId);
    } else {
      bigQueryClient.loadDataIntoTable(
          config,
          optimizedSourceUris,
          FormatOptions.avro(),
          writeDisposition,
          Optional.of(tableSchema));
    }
  }

  void updateMetadataIfNeeded() {
    BigQueryWriteHelper.updateTableMetadataIfNeeded(sparkSchema, config, bigQueryClient);
  }

  void cleanTemporaryGcsPathIfNeeded() {
    intermediateDataCleaner.ifPresent(cleaner -> cleaner.deletePath());
  }
}
