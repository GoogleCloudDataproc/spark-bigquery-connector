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
package com.google.cloud.spark.bigquery.v2.context;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class BigQueryDataSourceWriterModule implements Module {

  private final String writeUUID;
  private final StructType sparkSchema;
  private final SaveMode mode;

  public BigQueryDataSourceWriterModule(String writeUUID, StructType sparkSchema, SaveMode mode) {
    this.writeUUID = writeUUID;
    this.sparkSchema = sparkSchema;
    this.mode = mode;
  }

  @Override
  public void configure(Binder binder) {
    // empty
  }

  @Singleton
  @Provides
  public BigQueryDirectDataSourceWriterContext provideDirectDataSourceWriterContext(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      SparkBigQueryConfig config) {
    TableId tableId = config.getTableId();
    RetrySettings bigqueryDataWriteHelperRetrySettings =
        config.getBigqueryDataWriteHelperRetrySettings();
    return new BigQueryDirectDataSourceWriterContext(
        bigQueryClient,
        bigQueryWriteClientFactory,
        tableId,
        writeUUID,
        mode,
        sparkSchema,
        bigqueryDataWriteHelperRetrySettings);
  }

  @Singleton
  @Provides
  public BigQueryIndirectDataSourceWriterContext provideIndirectDataSourceWriterContext(
      BigQueryClient bigQueryClient, SparkBigQueryConfig config, SparkSession spark)
      throws IOException {
    Path gcsPath =
        createGcsPath(
            config,
            spark.sparkContext().hadoopConfiguration(),
            spark.sparkContext().applicationId());
    Optional<IntermediateDataCleaner> intermediateDataCleaner =
        config
            .getTemporaryGcsBucket()
            .map(
                ignored ->
                    new IntermediateDataCleaner(
                        gcsPath, spark.sparkContext().hadoopConfiguration()));
    // based on pmkc's suggestion at https://git.io/JeWRt
    intermediateDataCleaner.ifPresent(cleaner -> Runtime.getRuntime().addShutdownHook(cleaner));
    return new BigQueryIndirectDataSourceWriterContext(
        bigQueryClient,
        config,
        spark.sparkContext().hadoopConfiguration(),
        sparkSchema,
        writeUUID,
        mode,
        gcsPath,
        intermediateDataCleaner);
  }

  Path createGcsPath(SparkBigQueryConfig config, Configuration conf, String applicationId)
      throws IOException {
    Path gcsPath;
    Preconditions.checkArgument(
        config.getTemporaryGcsBucket().isPresent() || config.getPersistentGcsBucket().isPresent(),
        "Temporary or persistent GCS bucket must be informed.");

    // Throw exception if persistentGcsPath already exists in persistentGcsBucket
    if (config.getPersistentGcsBucket().isPresent() && config.getPersistentGcsPath().isPresent()) {
      gcsPath =
          new Path(
              String.format(
                  "gs://%s/%s",
                  config.getPersistentGcsBucket().get(), config.getPersistentGcsPath().get()));
      FileSystem fs = gcsPath.getFileSystem(conf);
      if (fs.exists(gcsPath)) {
        throw new IllegalArgumentException(
            String.format(
                "Path %s already exists in %s bucket",
                config.getPersistentGcsPath().get(), config.getPersistentGcsBucket().get()));
      }
    } else if (config.getTemporaryGcsBucket().isPresent()) {
      gcsPath =
          new Path(
              String.format(
                  "gs://%s/.spark-bigquery-%s-%s",
                  config.getTemporaryGcsBucket().get(), applicationId, UUID.randomUUID()));
    } else {
      gcsPath =
          new Path(
              String.format(
                  "gs://%s/.spark-bigquery-%s-%s",
                  config.getPersistentGcsBucket().get(), applicationId, UUID.randomUUID()));
    }

    return gcsPath;
  }
}
