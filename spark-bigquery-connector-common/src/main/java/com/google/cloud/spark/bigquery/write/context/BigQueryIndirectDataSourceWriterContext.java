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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.AvroSchemaConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryUtil;
import com.google.cloud.spark.bigquery.SupportedCustomDataType;
import com.google.cloud.spark.bigquery.write.IntermediateDataCleaner;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
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
      loadDataToBigQuery(sourceUris);
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

  void loadDataToBigQuery(List<String> sourceUris) throws IOException {
    // Solving Issue #248
    List<String> optimizedSourceUris = SparkBigQueryUtil.optimizeLoadUriListForSpark(sourceUris);
    JobInfo.WriteDisposition writeDisposition =
        SparkBigQueryUtil.saveModeToWriteDisposition(saveMode);

    Schema schema =
        SchemaConverters.from(SchemaConvertersConfiguration.from(config))
            .toBigQuerySchema(sparkSchema);
    if (tableInfo.isPresent()) {
      schema =
          BigQueryUtil.adjustSchemaIfNeeded(schema, tableInfo.get().getDefinition().getSchema());
    }

    bigQueryClient.loadDataIntoTable(
        config, optimizedSourceUris, FormatOptions.avro(), writeDisposition, Optional.of(schema));
  }

  void updateMetadataIfNeeded() {
    Map<String, StructField> fieldsToUpdate =
        Stream.of(sparkSchema.fields())
            .filter(
                field -> {
                  Optional<SupportedCustomDataType> supportedCustomDataType =
                      SupportedCustomDataType.of(field.dataType());
                  return supportedCustomDataType.isPresent()
                      || SchemaConverters.getDescriptionOrCommentOfField(
                              field, supportedCustomDataType)
                          .isPresent();
                })
            .collect(Collectors.toMap(StructField::name, Function.identity()));

    if (!fieldsToUpdate.isEmpty() || !config.getBigQueryTableLabels().isEmpty()) {
      TableInfo originalTableInfo = bigQueryClient.getTable(config.getTableIdWithoutThePartition());
      TableInfo.Builder updatedTableInfo = originalTableInfo.toBuilder();

      if (!fieldsToUpdate.isEmpty()) {
        logger.debug("updating schema, found fields to update: {}", fieldsToUpdate.keySet());
        TableDefinition originalTableDefinition = originalTableInfo.getDefinition();
        Schema originalSchema = originalTableDefinition.getSchema();
        Schema updatedSchema =
            Schema.of(
                originalSchema.getFields().stream()
                    .map(
                        field ->
                            Optional.ofNullable(fieldsToUpdate.get(field.getName()))
                                .map(sparkSchemaField -> updatedField(field, sparkSchemaField))
                                .orElse(field))
                    .collect(Collectors.toList()));
        updatedTableInfo.setDefinition(
            originalTableDefinition.toBuilder().setSchema(updatedSchema).build());
      }

      if (!config.getBigQueryTableLabels().isEmpty()) {
        updatedTableInfo.setLabels(config.getBigQueryTableLabels());
      }

      bigQueryClient.update(updatedTableInfo.build());
    }
  }

  Field updatedField(Field field, StructField sparkSchemaField) {
    Field.Builder newField = field.toBuilder();
    Optional<String> bqDescription =
        SchemaConverters.getDescriptionOrCommentOfField(
            sparkSchemaField, SupportedCustomDataType.of(sparkSchemaField.dataType()));
    bqDescription.ifPresent(newField::setDescription);
    return newField.build();
  }

  void cleanTemporaryGcsPathIfNeeded() {
    intermediateDataCleaner.ifPresent(cleaner -> cleaner.deletePath());
  }
}
