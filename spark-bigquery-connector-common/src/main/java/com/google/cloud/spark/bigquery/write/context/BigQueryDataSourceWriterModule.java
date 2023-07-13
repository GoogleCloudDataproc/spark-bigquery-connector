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

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryUtil;
import com.google.cloud.spark.bigquery.write.IntermediateDataCleaner;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class BigQueryDataSourceWriterModule implements Module {

  private final SparkBigQueryConfig tableConfig;
  private final String writeUUID;
  private final StructType sparkSchema;
  private final SaveMode mode;

  public BigQueryDataSourceWriterModule(
      SparkBigQueryConfig tableConfig, String writeUUID, StructType sparkSchema, SaveMode mode) {
    this.tableConfig = tableConfig;
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
      BigQueryClient bigQueryClient, BigQueryClientFactory bigQueryWriteClientFactory) {
    TableId tableId = tableConfig.getTableId();
    RetrySettings bigqueryDataWriteHelperRetrySettings =
        tableConfig.getBigqueryDataWriteHelperRetrySettings();
    return new BigQueryDirectDataSourceWriterContext(
        bigQueryClient,
        bigQueryWriteClientFactory,
        tableId,
        writeUUID,
        mode,
        sparkSchema,
        bigqueryDataWriteHelperRetrySettings,
        com.google.common.base.Optional.fromJavaUtil(tableConfig.getTraceId()),
        tableConfig.getEnableModeCheckForSchemaFields(),
        tableConfig.getBigQueryTableLabels(),
        SchemaConvertersConfiguration.from(tableConfig),
        tableConfig.getKmsKeyName(), // needs to be serializable
        tableConfig.isWriteAtLeastOnce());
  }

  @Singleton
  @Provides
  public BigQueryIndirectDataSourceWriterContext provideIndirectDataSourceWriterContext(
      BigQueryClient bigQueryClient, SparkSession spark) throws IOException {
    Path gcsPath =
        SparkBigQueryUtil.createGcsPath(
            tableConfig,
            spark.sparkContext().hadoopConfiguration(),
            spark.sparkContext().applicationId());
    Optional<IntermediateDataCleaner> intermediateDataCleaner =
        tableConfig
            .getTemporaryGcsBucket()
            .map(
                ignored ->
                    new IntermediateDataCleaner(
                        gcsPath, spark.sparkContext().hadoopConfiguration()));
    // based on pmkc's suggestion at https://git.io/JeWRt
    intermediateDataCleaner.ifPresent(cleaner -> Runtime.getRuntime().addShutdownHook(cleaner));
    return new BigQueryIndirectDataSourceWriterContext(
        bigQueryClient,
        tableConfig,
        spark.sparkContext().hadoopConfiguration(),
        sparkSchema,
        writeUUID,
        mode,
        gcsPath,
        intermediateDataCleaner);
  }
}
