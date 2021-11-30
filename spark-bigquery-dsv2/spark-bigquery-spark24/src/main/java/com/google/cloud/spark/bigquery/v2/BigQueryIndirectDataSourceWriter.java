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

import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.common.GenericBigQueryIndirectDataSourceWriter;
import com.google.cloud.spark.bigquery.common.IntermediateDataCleaner;
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
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DataSourceWriter implemented by first writing the DataFrame's data into GCS in an intermediate
 * format, and then triggering a BigQuery load job on this data. Hence the "indirect" - the data
 * goes through an intermediate storage.
 */
public class BigQueryIndirectDataSourceWriter implements DataSourceWriter {

  private static final Logger logger =
      LoggerFactory.getLogger(BigQueryIndirectDataSourceWriter.class);
  GenericBigQueryIndirectDataSourceWriter dataSourceWriterHelper;

  public BigQueryIndirectDataSourceWriter(
      BigQueryClient bigQueryClient,
      SparkBigQueryConfig config,
      Configuration hadoopConfiguration,
      StructType sparkSchema,
      String writeUUID,
      SaveMode saveMode,
      Path gcsPath,
      Optional<IntermediateDataCleaner> intermediateDataCleaner) {
    this.dataSourceWriterHelper =
        new GenericBigQueryIndirectDataSourceWriter(
            bigQueryClient,
            null,
            config,
            hadoopConfiguration,
            sparkSchema,
            writeUUID,
            saveMode,
            gcsPath,
            intermediateDataCleaner);
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new BigQueryIndirectDataWriterFactory(
        new SerializableConfiguration(this.dataSourceWriterHelper.getHadoopConfiguration()),
        this.dataSourceWriterHelper.getGcsPath().toString(),
        this.dataSourceWriterHelper.getSparkSchema(),
        this.dataSourceWriterHelper.getAvroSchemaName());
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
      String logMessage = this.dataSourceWriterHelper.updateMetadataIfNeeded();
      if (logMessage != null) {
        logger.debug("updated schema, following fields updated: {}", logMessage);
      } else {
        logger.debug("no fields updated");
      }
      logger.info("Data has been successfully loaded to BigQuery");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      this.dataSourceWriterHelper.cleanTemporaryGcsPathIfNeeded();
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    try {
      logger.warn(
          "Aborting write {} for table {}",
          this.dataSourceWriterHelper.getWriteUUID(),
          BigQueryUtil.friendlyTableName(this.dataSourceWriterHelper.getConfig().getTableId()));
    } finally {
      this.dataSourceWriterHelper.cleanTemporaryGcsPathIfNeeded();
    }
  }

  void loadDataToBigQuery(List<String> sourceUris) throws IOException {
    // Solving Issue #248
    Job finishedJob = this.dataSourceWriterHelper.loadDataToBigQuery(sourceUris);
    String jobResult = this.dataSourceWriterHelper.validateJobStatus(finishedJob);
    logger.info(
        jobResult,
        BigQueryUtil.friendlyTableName(this.dataSourceWriterHelper.getConfig().getTableId()),
        finishedJob.getJobId());
  }
}
