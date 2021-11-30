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
import com.google.cloud.spark.bigquery.common.GenericBigQueryIndirectDataSourceWriter;
import com.google.cloud.spark.bigquery.common.IntermediateDataCleaner;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public class BigQueryWriteBuilder implements WriteBuilder, SupportsOverwrite {
  private GenericBigQueryIndirectDataSourceWriter dataSourceWriterHelper;
  private final LogicalWriteInfo logicalWriteInfo;
  private boolean isDirectWrite;

  public BigQueryWriteBuilder(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      SparkBigQueryConfig config,
      Configuration conf,
      StructType sparkSchema,
      String writeUUID,
      SaveMode mode,
      Path gcsPath,
      Optional<IntermediateDataCleaner> intermediateDataCleaner,
      boolean isDirectWrite,
      LogicalWriteInfo logicalWriteInfo) {
    this.logicalWriteInfo = logicalWriteInfo;
    this.dataSourceWriterHelper =
        new GenericBigQueryIndirectDataSourceWriter(
            bigQueryClient,
            bigQueryWriteClientFactory,
            config,
            conf,
            sparkSchema,
            writeUUID,
            mode,
            gcsPath,
            intermediateDataCleaner);
    this.isDirectWrite = isDirectWrite;
  }

  @Override
  public BatchWrite buildForBatch() {
    if (this.isDirectWrite) {
      return new BigQueryDirectBatchWriter(
          this.dataSourceWriterHelper.getBigQueryClient(),
          this.dataSourceWriterHelper.getBigQueryWriteClientFactory(),
          this.dataSourceWriterHelper.getConfig().getTableId(),
          this.dataSourceWriterHelper.getWriteUUID(),
          this.dataSourceWriterHelper.getSaveMode(),
          this.dataSourceWriterHelper.getSparkSchema(),
          this.dataSourceWriterHelper.getConfig());
    } else
      return new BigQueryIndirectBatchWriter(
          this.dataSourceWriterHelper.getBigQueryClient(),
          this.dataSourceWriterHelper.getConfig(),
          this.dataSourceWriterHelper.getHadoopConfiguration(),
          this.dataSourceWriterHelper.getSparkSchema(),
          this.dataSourceWriterHelper.getWriteUUID(),
          this.dataSourceWriterHelper.getSaveMode(),
          this.dataSourceWriterHelper.getGcsPath(),
          this.dataSourceWriterHelper.getIntermediateDataCleaner(),
          this.logicalWriteInfo);
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    this.dataSourceWriterHelper.setMode(SaveMode.Overwrite);
    return this;
  }
}
