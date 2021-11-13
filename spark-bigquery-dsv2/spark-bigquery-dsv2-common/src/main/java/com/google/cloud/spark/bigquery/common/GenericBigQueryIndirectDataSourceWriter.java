package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.cloud.spark.bigquery.*;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class GenericBigQueryIndirectDataSourceWriter implements Serializable {
  private final BigQueryClient bigQueryClient;
  private final SparkBigQueryConfig config;
  private final Configuration hadoopConfiguration;
  private final String writeUUID;
  private final Path gcsPath;
  private final StructType sparkSchema;
  private SaveMode saveMode;
  private final Optional<IntermediateDataCleaner> intermediateDataCleaner;
  private final org.apache.avro.Schema avroSchema;

  public GenericBigQueryIndirectDataSourceWriter(
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
    this.avroSchema = AvroSchemaConverter.sparkSchemaToAvroSchema(sparkSchema);
  }

  public BigQueryClient getBigQueryClient() {
    return bigQueryClient;
  }

  public SparkBigQueryConfig getConfig() {
    return config;
  }

  public Configuration getHadoopConfiguration() {
    return hadoopConfiguration;
  }

  public String getWriteUUID() {
    return writeUUID;
  }

  public Path getGcsPath() {
    return gcsPath;
  }

  public StructType getSparkSchema() {
    return sparkSchema;
  }

  public SaveMode getSaveMode() {
    return saveMode;
  }

  public Optional<IntermediateDataCleaner> getIntermediateDataCleaner() {
    return intermediateDataCleaner;
  }

  public void setMode(SaveMode mode) {
    this.saveMode = mode;
  }

  public LoadJobConfiguration.Builder createJobConfiguration(List<String> sourceUris) {
    return LoadJobConfiguration.newBuilder(
            this.config.getTableId(),
            sourceUris,
            this.config.getIntermediateFormat().getFormatOptions())
        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
        .setAutodetect(true);
  }

  public LoadJobConfiguration.Builder prepareJobConfiguration(
      LoadJobConfiguration.Builder jobConfiguration) {
    this.config.getCreateDisposition().ifPresent(jobConfiguration::setCreateDisposition);

    if (this.config.getPartitionField().isPresent() || this.config.getPartitionType().isPresent()) {
      TimePartitioning.Builder timePartitionBuilder =
          TimePartitioning.newBuilder(this.config.getPartitionTypeOrDefault());
      this.config.getPartitionExpirationMs().ifPresent(timePartitionBuilder::setExpirationMs);
      this.config
          .getPartitionRequireFilter()
          .ifPresent(timePartitionBuilder::setRequirePartitionFilter);
      this.config.getPartitionField().ifPresent(timePartitionBuilder::setField);
      jobConfiguration.setTimePartitioning(timePartitionBuilder.build());
      this.config
          .getClusteredFields()
          .ifPresent(
              clusteredFields -> {
                Clustering clustering = Clustering.newBuilder().setFields(clusteredFields).build();
                jobConfiguration.setClustering(clustering);
              });
    }

    if (!this.config.getLoadSchemaUpdateOptions().isEmpty()) {
      jobConfiguration.setSchemaUpdateOptions(getConfig().getLoadSchemaUpdateOptions());
    }
    return jobConfiguration;
  }

  public String validateJobStatus(Job finishedJob) {
    if (finishedJob.getStatus().getError() != null) {
      throw new BigQueryException(
          BaseHttpServiceException.UNKNOWN_CODE,
          String.format(
              "Failed to load to %s in job %s. BigQuery error was '%s'",
              BigQueryUtil.friendlyTableName(getConfig().getTableId()),
              finishedJob.getJobId(),
              finishedJob.getStatus().getError().getMessage()),
          finishedJob.getStatus().getError());
    } else {
      return "Done loading to {}. jobId: {}";
    }
  }

  public String getAvroSchemaName() {
    return this.avroSchema.toString();
  }

  public void cleanTemporaryGcsPathIfNeeded() {
    this.intermediateDataCleaner.ifPresent(cleaner -> cleaner.run());
  }

  public Job loadDataToBigQuery(List<String> sourceUris) throws IOException {
    // Solving Issue #248
    List<String> optimizedSourceUris = SparkBigQueryUtil.optimizeLoadUriListForSpark(sourceUris);

    LoadJobConfiguration.Builder jobConfiguration = createJobConfiguration(optimizedSourceUris);

    jobConfiguration.setWriteDisposition(saveModeToWriteDisposition(saveMode));
    Job finishedJob =
        this.bigQueryClient.createAndWaitFor(prepareJobConfiguration(jobConfiguration));
    return finishedJob;
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

  public String updateMetadataIfNeeded() {
    Map<String, StructField> fieldsToUpdate =
        Stream.of(sparkSchema.fields())
            .filter(
                field ->
                    SupportedCustomDataType.of(field.dataType()).isPresent()
                        || SchemaConverters.getDescriptionOrCommentOfField(field).isPresent())
            .collect(Collectors.toMap(StructField::name, Function.identity()));

    if (!fieldsToUpdate.isEmpty()) {
      TableInfo originalTableInfo = bigQueryClient.getTable(config.getTableIdWithoutThePartition());
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

      bigQueryClient.update(updatedTableInfo.build());
      return fieldsToUpdate.keySet().toString();
    } else {
      return null;
    }
  }
}
