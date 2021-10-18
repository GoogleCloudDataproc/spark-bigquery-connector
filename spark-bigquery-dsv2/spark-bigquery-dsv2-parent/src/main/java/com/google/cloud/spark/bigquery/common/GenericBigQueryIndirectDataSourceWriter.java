package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class GenericBigQueryIndirectDataSourceWriter {
  private final BigQueryClient bigQueryClient;
  private final SparkBigQueryConfig config;
  private final Configuration hadoopConfiguration;
  private final String writeUUID;
  private final Path gcsPath;

  public GenericBigQueryIndirectDataSourceWriter(
      BigQueryClient bigQueryClient,
      SparkBigQueryConfig config,
      Configuration hadoopConfiguration,
      String writeUUID,
      Path gcsPath) {
    this.bigQueryClient = bigQueryClient;
    this.config = config;
    this.hadoopConfiguration = hadoopConfiguration;
    this.writeUUID = writeUUID;
    this.gcsPath = gcsPath;
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
}
