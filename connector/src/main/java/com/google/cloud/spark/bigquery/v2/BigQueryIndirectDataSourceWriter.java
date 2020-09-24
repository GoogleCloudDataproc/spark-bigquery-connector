package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SupportedCustomDataType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class BigQueryIndirectDataSourceWriter implements DataSourceWriter {

  private static final Logger logger =
      LoggerFactory.getLogger(BigQueryIndirectDataSourceWriter.class);

  private final BigQueryClient bigQueryClient;
  private final SparkBigQueryConfig config;
  private final Configuration hadoopConfiguration;
  private final StructType sparkSchema;
  private final String writeUUID;
  private final SaveMode saveMode;
  private final Path gcsPath;
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
    this.bigQueryClient = bigQueryClient;
    this.config = config;
    this.hadoopConfiguration = hadoopConfiguration;
    this.sparkSchema = sparkSchema;
    this.writeUUID = writeUUID;
    this.saveMode = saveMode;
    this.gcsPath = gcsPath;
    this.intermediateDataCleaner = intermediateDataCleaner;
  }

  static <T> Iterable<T> wrap(final RemoteIterator<T> remoteIterator) {
    return () ->
        new Iterator<T>() {
          @Override
          public boolean hasNext() {
            try {
              return remoteIterator.hasNext();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }

          @Override
          public T next() {
            try {
              return remoteIterator.next();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        };
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new BigQueryIndirectDataWriterFactory();
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    try {
      loadDataToBigQuery();
      updateMetadataIfNeeded();
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
          writeUUID,
          BigQueryUtil.friendlyTableName(config.getTableId()));
    } finally {
      cleanTemporaryGcsPathIfNeeded();
    }
  }

  void loadDataToBigQuery() throws IOException {
    FileSystem fs = gcsPath.getFileSystem(hadoopConfiguration);
    List<String> sourceUris =
        StreamSupport.stream(wrap(fs.listFiles(gcsPath, false)).spliterator(), false)
            .map(file -> file.getPath().toString())
            .filter(
                path ->
                    path.toLowerCase()
                        .endsWith(
                            config
                                .getIntermediateFormat()
                                .getFormatOptions()
                                .toString()
                                .toLowerCase()))
            .collect(Collectors.toList());

    LoadJobConfiguration.Builder jobConfiguration =
        LoadJobConfiguration.newBuilder(
                config.getTableId(), sourceUris, config.getIntermediateFormat().getFormatOptions())
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .setWriteDisposition(saveModeToWriteDisposition(saveMode))
            .setAutodetect(true);

    config.getCreateDisposition().ifPresent(jobConfiguration::setCreateDisposition);

    if (config.getPartitionField().isPresent() || config.getPartitionType().isPresent()) {
      TimePartitioning.Builder timePartitionBuilder =
          TimePartitioning.newBuilder(
              TimePartitioning.Type.valueOf(config.getPartitionType().orElse("DAY")));
      config.getPartitionExpirationMs().ifPresent(timePartitionBuilder::setExpirationMs);
      config.getPartitionRequireFilter().ifPresent(timePartitionBuilder::setRequirePartitionFilter);
      config.getPartitionField().ifPresent(timePartitionBuilder::setField);
      jobConfiguration.setTimePartitioning(timePartitionBuilder.build());
      config
          .getClusteredFields()
          .ifPresent(
              clusteredFields -> {
                Clustering clustering = Clustering.newBuilder().setFields(clusteredFields).build();
                jobConfiguration.setClustering(clustering);
              });
    }

    if (!config.getLoadSchemaUpdateOptions().isEmpty()) {
      jobConfiguration.setSchemaUpdateOptions(config.getLoadSchemaUpdateOptions());
    }

    Job finishedJob = bigQueryClient.createAndWaitFor(jobConfiguration);

    if (finishedJob.getStatus().getError() != null) {
      throw new BigQueryException(
          BaseHttpServiceException.UNKNOWN_CODE,
          String.format(
              "Failed to load to %s in job %s. BigQuery error was '%s'",
              BigQueryUtil.friendlyTableName(config.getTableId()),
              finishedJob.getJobId(),
              finishedJob.getStatus().getError().getMessage()),
          finishedJob.getStatus().getError());
    } else {
      logger.info(
          "Done loading to %s. jobId: %s",
          BigQueryUtil.friendlyTableName(config.getTableId()), finishedJob.getJobId());
    }
  }

  JobInfo.WriteDisposition saveModeToWriteDisposition(SaveMode saveMode) {
    if (saveMode == SaveMode.Append) {
      return JobInfo.WriteDisposition.WRITE_APPEND;
    }
    if (saveMode == SaveMode.Overwrite) {
      return JobInfo.WriteDisposition.WRITE_TRUNCATE;
    }
    throw new UnsupportedOperationException(
        "SaveMode " + saveMode + " is currently not supported.");
  }

  void updateMetadataIfNeeded() {
    // TODO: Issue #190 should be solved here
    Map<String, Optional<SupportedCustomDataType>> fieldsToUpdate =
        Stream.of(sparkSchema.fields())
            .map(
                field ->
                    new AbstractMap.SimpleImmutableEntry<String, Optional<SupportedCustomDataType>>(
                        field.name(), SupportedCustomDataType.of(field.dataType())))
            .filter(nameAndType -> nameAndType.getValue().isPresent())
            .collect(
                Collectors.toMap(
                    AbstractMap.SimpleImmutableEntry::getKey,
                    AbstractMap.SimpleImmutableEntry::getValue));
    if (!fieldsToUpdate.isEmpty()) {
      logger.debug("updating schema, found fields to update: {}", fieldsToUpdate.keySet());
      TableInfo originalTableInfo = bigQueryClient.getTable(config.getTableId());
      TableDefinition originalTableDefinition = originalTableInfo.getDefinition();
      Schema originalSchema = originalTableDefinition.getSchema();
      Schema updatedSchema =
          Schema.of(
              originalSchema.getFields().stream()
                  .map(
                      field ->
                          fieldsToUpdate
                              .get(field.getName())
                              .map(dataType -> updatedField(field, dataType.getTypeMarker()))
                              .orElse(field))
                  .collect(Collectors.toList()));
      TableInfo.Builder updatedTableInfo =
          originalTableInfo
              .toBuilder()
              .setDefinition(originalTableDefinition.toBuilder().setSchema(updatedSchema).build());

      bigQueryClient.update(updatedTableInfo.build());
    }
  }

  Field updatedField(Field field, String marker) {
    Field.Builder newField = field.toBuilder();
    String description = field.getDescription();
    if (description == null) {
      newField.setDescription(marker);
    } else if (!description.endsWith(marker)) {
      newField.setDescription(description + " " + marker);
    }
    return newField.build();
  }

  void cleanTemporaryGcsPathIfNeeded() {
    intermediateDataCleaner.ifPresent(cleaner -> cleaner.deletePath());
  }
}
