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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.AvroSchemaConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryUtil;
import com.google.cloud.spark.bigquery.SupportedCustomDataType;
import com.google.cloud.spark.bigquery.common.GenericBigQueryIndirectDataSourceWriter;
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
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DataSourceWriter implemented by first writing the DataFrame's data into GCS in an intermediate
 * format, and then triggering a BigQuery load job on this data. Hence the "indirect" - the data
 * goes through an intermediate storage.
 */
public class BigQueryIndirectDataSourceWriter extends GenericBigQueryIndirectDataSourceWriter
    implements DataSourceWriter {

  private static final Logger logger =
      LoggerFactory.getLogger(BigQueryIndirectDataSourceWriter.class);

  private final StructType sparkSchema;
  private final SaveMode saveMode;
  private final Optional<IntermediateDataCleaner> intermediateDataCleaner;

  public BigQueryIndirectDataSourceWriter(
      BigQueryClient bigQueryClient,
      SparkBigQueryConfig config,
      Configuration hadoopConfiguration,
      StructType sparkSchema,
      String writeUUID,
      SaveMode saveMode,
      Path gcsPath,
      Optional<IntermediateDataCleaner> intermediateDataCleaner) {
    super(bigQueryClient, config, hadoopConfiguration, writeUUID, gcsPath);
    this.sparkSchema = sparkSchema;
    this.saveMode = saveMode;
    this.intermediateDataCleaner = intermediateDataCleaner;
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    org.apache.avro.Schema avroSchema = AvroSchemaConverter.sparkSchemaToAvroSchema(sparkSchema);
    return new BigQueryIndirectDataWriterFactory(
        new SerializableConfiguration(getHadoopConfiguration()),
        getGcsPath().toString(),
        sparkSchema,
        avroSchema.toString());
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    logger.info(
        "Data has been successfully written to GCS. Going to load {} files to BigQuery",
        messages.length);
    try {
      List<String> sourceUris =
          Stream.of(messages)
              .map(msg -> ((BigQueryIndirectWriterCommitMessage) msg).getUri())
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
  public void abort(WriterCommitMessage[] messages) {
    try {
      logger.warn(
          "Aborting write {} for table {}",
          getWriteUUID(),
          BigQueryUtil.friendlyTableName(getConfig().getTableId()));
    } finally {
      cleanTemporaryGcsPathIfNeeded();
    }
  }

  void loadDataToBigQuery(List<String> sourceUris) throws IOException {
    // Solving Issue #248
    List<String> optimizedSourceUris = SparkBigQueryUtil.optimizeLoadUriListForSpark(sourceUris);

    LoadJobConfiguration.Builder jobConfiguration = createJobConfiguration(optimizedSourceUris);
    jobConfiguration.setWriteDisposition(saveModeToWriteDisposition(saveMode));
    Job finishedJob =
        getBigQueryClient().createAndWaitFor(prepareJobConfiguration(jobConfiguration));
    String jobResult = validateJobStatus(finishedJob);
    logger.info(
        jobResult,
        BigQueryUtil.friendlyTableName(getConfig().getTableId()),
        finishedJob.getJobId());
  }

  JobInfo.WriteDisposition saveModeToWriteDisposition(SaveMode saveMode) {
    if (saveMode == SaveMode.ErrorIfExists) {
      return JobInfo.WriteDisposition.WRITE_EMPTY;
    }
    // SaveMode.Ignore is handled in the data source level. If it has arrived here it means tha
    // table does not exist
    if (saveMode == SaveMode.Append || saveMode == SaveMode.Ignore) {
      return JobInfo.WriteDisposition.WRITE_APPEND;
    }
    if (saveMode == SaveMode.Overwrite) {
      return JobInfo.WriteDisposition.WRITE_TRUNCATE;
    }
    throw new UnsupportedOperationException(
        "SaveMode " + saveMode + " is currently not supported.");
  }

  void updateMetadataIfNeeded() {
    Map<String, StructField> fieldsToUpdate =
        Stream.of(sparkSchema.fields())
            .filter(
                field ->
                    SupportedCustomDataType.of(field.dataType()).isPresent()
                        || SchemaConverters.getDescriptionOrCommentOfField(field).isPresent())
            .collect(Collectors.toMap(StructField::name, Function.identity()));

    if (!fieldsToUpdate.isEmpty()) {
      logger.debug("updating schema, found fields to update: {}", fieldsToUpdate.keySet());
      TableInfo originalTableInfo =
          getBigQueryClient().getTable(getConfig().getTableIdWithoutThePartition());
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
      TableInfo.Builder updatedTableInfo =
          originalTableInfo
              .toBuilder()
              .setDefinition(originalTableDefinition.toBuilder().setSchema(updatedSchema).build());

      getBigQueryClient().update(updatedTableInfo.build());
    }
  }

  Field updatedField(Field field, StructField sparkSchemaField) {
    Field.Builder newField = field.toBuilder();
    Optional<String> bqDescription =
        SchemaConverters.getDescriptionOrCommentOfField(sparkSchemaField);

    if (bqDescription.isPresent()) {
      newField.setDescription(bqDescription.get());
    } else {
      String description = field.getDescription();
      String marker = SupportedCustomDataType.of(sparkSchemaField.dataType()).get().getTypeMarker();

      if (description == null) {
        newField.setDescription(marker);
      } else if (!description.endsWith(marker)) {
        newField.setDescription(description + " " + marker);
      }
    }
    return newField.build();
  }

  void cleanTemporaryGcsPathIfNeeded() {
    intermediateDataCleaner.ifPresent(cleaner -> cleaner.run());
  }
}
