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
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderModule;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
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
      ImmutableSet.of(
          TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.TRUNCATE);

  private Injector injector;
  private TableInfo tableInfo;
  private StructType schema;

  private BigQueryTable(Injector injector, TableInfo tableInfo, StructType schema) {
    this.injector = injector;
    this.tableInfo = tableInfo;
    this.schema = schema;
  }

  public static BigQueryTable fromConfigurationAndSchema(
      Injector injector, StructType sparkProvidedSchema) throws NoSuchTableException {
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    return createInternal(injector, config.getTableId(), sparkProvidedSchema);
  }

  public static BigQueryTable fromIdentifier(Injector injector, Identifier ident)
      throws NoSuchTableException {
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    return createInternal(
        injector, ((BigQueryIdentifier) ident).getTableId(), /*sparkProvidedSchema*/ null);
  }

  public static BigQueryTable fromTableInfo(Injector injector, TableInfo tableInfo) {
    return new BigQueryTable(
        injector, tableInfo, SchemaConverters.toSpark(tableInfo.getDefinition().getSchema()));
  }

  private static BigQueryTable createInternal(
      Injector injector, TableId tableId, StructType sparkProvidedSchema)
      throws NoSuchTableException {
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    TableInfo tableInfo = bigQueryClient.getTable(tableId);
    if (tableInfo == null) {
      throw new NoSuchTableException(new BigQueryIdentifier(tableId));
    }
    StructType schema =
        sparkProvidedSchema != null
            ? sparkProvidedSchema
            : SchemaConverters.toSpark(tableInfo.getDefinition().getSchema());
    return new BigQueryTable(injector, tableInfo, schema);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    SparkBigQueryConfig tableScanConfig =
        SparkBigQueryConfig.from(
            options,
            injector.getInstance(DataSourceVersion.class),
            injector.getInstance(SparkSession.class),
            Optional.of(schema), /*tableIsMandatory*/
            true);
    Injector readerInjector =
        injector.createChildInjector(
            new BigQueryDataSourceReaderModule(Optional.of(tableScanConfig)));
    BigQueryDataSourceReaderContext ctx =
        readerInjector.getInstance(BigQueryDataSourceReaderContext.class);
    return new BigQueryScanBuilder(ctx);
  }

  @Override
  public String name() {
    TableId tableId = injector.getInstance(SparkBigQueryConfig.class).getTableId();
    if (tableId == null) {
      return "nonexistingtable";
    }
    return tableId.getTable();
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

  TableId getTableId() {
    return tableInfo.getTableId();
  }
}
