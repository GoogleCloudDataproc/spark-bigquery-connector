/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderModule;
import com.google.cloud.spark.bigquery.v2.context.DataSourceWriterContext;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class BigQueryTable implements Table, SupportsRead, SupportsWrite {

  public static final ImmutableSet<TableCapability> TABLE_CAPABILITIES =
      ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);

  private Injector injector;

  public BigQueryTable(Injector injector) {
    this.injector = injector;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    Injector readerInjector = injector.createChildInjector(new BigQueryDataSourceReaderModule());
    BigQueryDataSourceReaderContext ctx =
        readerInjector.getInstance(BigQueryDataSourceReaderContext.class);
    return new BigQueryScanBuilder(ctx);
  }

  @Override
  public String name() {
    return injector.getInstance(SparkBigQueryConfig.class).getTableId().getTable();
  }

  @Override
  public StructType schema() {
    return injector.getInstance(StructType.class);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return TABLE_CAPABILITIES;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    CaseInsensitiveStringMap options = info.options();
    SaveMode mode = SaveMode.valueOf(options.get("mode"));
    Optional<DataSourceWriterContext> dataSourceWriterContext =
        DataSourceWriterContext.create(injector, info.queryId(), info.schema(), mode, options);
    // The case where mode == SaveMode.Ignore is handled by Spark, so we can assume we can get the
    // context
    return new BigQueryWriteBuilder(dataSourceWriterContext.get());
  }
}
