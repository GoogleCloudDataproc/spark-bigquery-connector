/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.write;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryUtil;
import com.google.cloud.spark.bigquery.SupportedCustomDataType;
import com.google.cloud.spark.bigquery.util.HdfsUtils;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryWriteHelper {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryWriteHelper.class);

  private final BigQueryClient bigQueryClient;
  private final SaveMode saveMode;
  private final SparkBigQueryConfig config;
  private final Dataset<Row> data;
  private final Optional<TableInfo> tableInfo;
  private final Configuration conf;
  private final Path gcsPath;
  private final Optional<IntermediateDataCleaner> createTemporaryPathDeleter;

  public BigQueryWriteHelper(
      BigQueryClient bigQueryClient,
      SQLContext sqlContext,
      SaveMode saveMode,
      SparkBigQueryConfig config,
      Dataset<Row> data,
      Optional<TableInfo> tableInfo) {
    this.bigQueryClient = bigQueryClient;
    verifySaveMode(saveMode);
    this.saveMode = saveMode;
    this.config = config;
    this.data = data;
    this.tableInfo = tableInfo;
    this.conf = sqlContext.sparkContext().hadoopConfiguration();
    this.gcsPath =
        SparkBigQueryUtil.createGcsPath(config, conf, sqlContext.sparkContext().applicationId());
    this.createTemporaryPathDeleter =
        config.getTemporaryGcsBucket().map(unused -> new IntermediateDataCleaner(gcsPath, conf));
  }

  public void writeDataFrameToBigQuery() {
    // If the CreateDisposition is CREATE_NEVER, and the table does not exist,
    // there's no point in writing the data to GCS in the first place as it going
    // to file on the BigQuery side.
    if (config
        .getCreateDisposition()
        .map(cd -> !tableExists() && cd == JobInfo.CreateDisposition.CREATE_NEVER)
        .orElse(false)) {
      throw new BigQueryConnectorException(
          String.format(
              "For table %s Create Disposition is CREATE_NEVER and the table does not exists. Aborting the insert",
              friendlyTableName()));
    }

    try {
      // based on pmkc's suggestion at https://git.io/JeWRt
      createTemporaryPathDeleter.ifPresent(
          cleaner -> Runtime.getRuntime().addShutdownHook(cleaner));

      String format = config.getIntermediateFormat().getDataSource();
      data.write().format(format).save(gcsPath.toString());

      loadDataToBigQuery();
      updateMetadataIfNeeded();
    } catch (Exception e) {
      throw new BigQueryConnectorException("Failed to write to BigQuery", e);
    } finally {
      cleanTemporaryGcsPathIfNeeded();
    }
  }

  void loadDataToBigQuery() throws IOException {
    FileSystem fs = gcsPath.getFileSystem(conf);
    FormatOptions formatOptions = config.getIntermediateFormat().getFormatOptions();
    String suffix = "." + formatOptions.getType().toLowerCase();
    List<String> sourceUris =
        SparkBigQueryUtil.optimizeLoadUriListForSpark(
            Streams.stream(HdfsUtils.toJavaUtilIterator(fs.listFiles(gcsPath, false)))
                .map(file -> file.getPath().toString())
                .filter(path -> path.toLowerCase().endsWith(suffix))
                .collect(Collectors.toList()));
    // Solving Issue #248
    List<String> optimizedSourceUris = SparkBigQueryUtil.optimizeLoadUriListForSpark(sourceUris);
    JobInfo.WriteDisposition writeDisposition =
        SparkBigQueryUtil.saveModeToWriteDisposition(saveMode);
    Schema schema =
        SchemaConverters.from(SchemaConvertersConfiguration.from(config))
            .toBigQuerySchema(data.schema());
    if (tableInfo.isPresent()) {
      schema =
          BigQueryUtil.adjustSchemaIfNeeded(schema, tableInfo.get().getDefinition().getSchema());
    }

    bigQueryClient.loadDataIntoTable(
        config, optimizedSourceUris, formatOptions, writeDisposition, Optional.of(schema));
  }

  String friendlyTableName() {
    return BigQueryUtil.friendlyTableName(config.getTableId());
  }

  void updateMetadataIfNeeded() {
    updateTableMetadataIfNeeded(data.schema(), config, bigQueryClient);
  }

  public static void updateTableMetadataIfNeeded(
      StructType sparkSchema, SparkBigQueryConfig config, BigQueryClient bigQueryClient) {
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
        updatedTableInfo.setLabels(config.getBigQueryTableLabels()).build();
      }

      bigQueryClient.update(updatedTableInfo.build());
    }
  }

  static Field updatedField(Field field, StructField sparkSchemaField) {
    Field.Builder newField = field.toBuilder();
    Optional<String> bqDescription =
        SchemaConverters.getDescriptionOrCommentOfField(
            sparkSchemaField, SupportedCustomDataType.of(sparkSchemaField.dataType()));
    bqDescription.ifPresent(newField::setDescription);
    return newField.build();
  }

  void cleanTemporaryGcsPathIfNeeded() {
    // TODO(davidrab): add flag to disable the deletion?
    createTemporaryPathDeleter.ifPresent(IntermediateDataCleaner::deletePath);
  }

  static void verifySaveMode(SaveMode saveMode) {
    if (saveMode == SaveMode.ErrorIfExists || saveMode == SaveMode.Ignore) {
      throw new UnsupportedOperationException("SaveMode " + saveMode + " is not supported");
    }
  }

  private boolean tableExists() {
    return tableInfo.isPresent();
  }
}
