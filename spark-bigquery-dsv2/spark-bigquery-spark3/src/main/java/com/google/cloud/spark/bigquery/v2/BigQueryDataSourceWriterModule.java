/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDataSourceWriterModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.IOException;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.StructType;

class BigQueryDataSourceWriterModule implements Module {
  private GenericBigQueryDataSourceWriterModule dataSourceWriterModuleHelper;
  private final LogicalWriteInfo logicalWriteInfo;
  private SaveMode mode;
  private boolean isDirectWrite;

  BigQueryDataSourceWriterModule(
      String writeUUID,
      StructType sparkSchema,
      SaveMode mode,
      boolean isDirectWrite,
      LogicalWriteInfo logicalWriteInfo,
      BigQueryClient bigQueryClient,
      SparkBigQueryConfig config) {
    this.mode = mode;
    this.dataSourceWriterModuleHelper =
        new GenericBigQueryDataSourceWriterModule(writeUUID, sparkSchema, this.mode);
    this.logicalWriteInfo = logicalWriteInfo;
    this.isDirectWrite = isDirectWrite;
  }

  @Override
  public void configure(Binder binder) {
    // empty
  }

  @Singleton
  @Provides
  public BigQueryWriteBuilder provideDataSourceWriter(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      SparkBigQueryConfig config,
      SparkSession spark)
      throws IOException {
    if (!isDirectWrite)
      this.dataSourceWriterModuleHelper.createIntermediateCleaner(
          config, spark.sparkContext().hadoopConfiguration(), spark.sparkContext().applicationId());
    return new BigQueryWriteBuilder(
        bigQueryClient,
        bigQueryWriteClientFactory,
        config,
        spark.sparkContext().hadoopConfiguration(),
        this.dataSourceWriterModuleHelper.getSparkSchema(),
        this.dataSourceWriterModuleHelper.getWriteUUID(),
        this.dataSourceWriterModuleHelper.getMode(),
        this.dataSourceWriterModuleHelper.getGcsPath(),
        this.dataSourceWriterModuleHelper.getIntermediateDataCleaner(),
        this.isDirectWrite,
        this.logicalWriteInfo);
  }
}
