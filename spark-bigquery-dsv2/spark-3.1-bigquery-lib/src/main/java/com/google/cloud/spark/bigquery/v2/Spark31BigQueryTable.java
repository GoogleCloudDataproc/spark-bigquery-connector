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

import com.google.cloud.bigquery.TableId;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class Spark31BigQueryTable implements Table, SupportsRead, SupportsWrite {

  public static final ImmutableSet<TableCapability> TABLE_CAPABILITIES =
      ImmutableSet.of(
          TableCapability.BATCH_READ, TableCapability.V1_BATCH_WRITE, TableCapability.TRUNCATE);

  protected Injector injector;
  protected TableId tableId;
  protected StructType schema;

  public Spark31BigQueryTable(Injector injector, TableId tableId, StructType schema) {
    this.injector = injector;
    this.tableId = tableId;
    this.schema = schema;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    BigQueryDataSourceReaderContext ctx = createBigQueryDataSourceReaderContext(options);
    return new Spark31BigQueryScanBuilder(ctx);
  }

  protected BigQueryDataSourceReaderContext createBigQueryDataSourceReaderContext(
      CaseInsensitiveStringMap options) {
    SparkBigQueryConfig tableScanConfig =
        SparkBigQueryConfig.from(
            options,
            ImmutableMap.of(),
            injector.getInstance(DataSourceVersion.class),
            injector.getInstance(SparkSession.class),
            Optional.of(schema), /*tableIsMandatory*/
            true);
    Injector readerInjector =
        injector.createChildInjector(
            new BigQueryDataSourceReaderModule(Optional.of(tableScanConfig)));
    BigQueryDataSourceReaderContext ctx =
        readerInjector.getInstance(BigQueryDataSourceReaderContext.class);
    return ctx;
  }

  @Override
  public String name() {
    return injector.getInstance(SparkBigQueryConfig.class).getTableId().getTable();
  }

  @Override
  public StructType schema() {
    return this.schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return TABLE_CAPABILITIES;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    // SaveMode is not provided by spark 3, it is handled by the DataFrameWriter
    // The case where mode == SaveMode.Ignore is handled by Spark, so we can assume we can get the
    // context
    return new BigQueryWriteBuilder(injector, info, SaveMode.Append);
  }
}
