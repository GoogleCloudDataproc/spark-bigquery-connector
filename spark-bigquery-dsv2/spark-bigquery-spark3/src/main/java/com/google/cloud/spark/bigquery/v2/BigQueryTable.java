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
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.common.BigQueryDataSourceHelper;
import com.google.inject.Injector;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class BigQueryTable implements SupportsRead, SupportsWrite {
  private Set<TableCapability> capabilities;
  private final StructType schema;
  private final Transform[] partitioning;
  private final Map<String, String> properties;
  private final BigQueryClient bigQueryClient;
  private final SparkBigQueryConfig config;
  private final SparkSession spark;
  private BigQueryDataSourceHelper dataSourceHelper = new BigQueryDataSourceHelper();

  @Override
  public Transform[] partitioning() {
    return this.partitioning;
  }

  public BigQueryTable(
      StructType schema,
      Transform[] partitioning,
      Map<String, String> properties,
      BigQueryClient bigQueryClient,
      SparkBigQueryConfig config,
      SparkSession spark)
      throws AnalysisException {
    this.schema = schema;
    this.partitioning = partitioning;
    this.properties = properties;
    this.bigQueryClient = bigQueryClient;
    this.config = config;
    this.spark = spark;
  }

  @Override
  public Map<String, String> properties() {
    return this.properties;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    Map<String, String> props = new HashMap<>(options);
    Injector injector =
        this.dataSourceHelper.createInjector(
            this.schema, props, false, null, new BigQueryScanBuilderModule());
    BigQueryScanBuilder bqScanBuilder = injector.getInstance(BigQueryScanBuilder.class);
    return bqScanBuilder;
  }

  @Override
  public String name() {
    return this.config.getTableId().getTable();
  }

  @Override
  public StructType schema() {
    return new StructType(this.schema.fields());
  }

  @Override
  public Set<TableCapability> capabilities() {
    if (capabilities == null) {
      capabilities = new HashSet<TableCapability>();
      capabilities.add(TableCapability.BATCH_READ);
      capabilities.add(TableCapability.BATCH_WRITE);
      capabilities.add(TableCapability.TRUNCATE);
      capabilities.add(TableCapability.OVERWRITE_BY_FILTER);
      capabilities.add(TableCapability.OVERWRITE_DYNAMIC);
    }
    return capabilities;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
    Map<String, String> props = new HashMap<>(logicalWriteInfo.options());
    Injector injector;
    injector =
        this.dataSourceHelper.createInjector(
            logicalWriteInfo.schema(),
            props,
            this.dataSourceHelper.isDirectWrite(props.get("writePath")),
            null,
            new BigQueryDataSourceWriterModule(
                logicalWriteInfo.queryId(),
                logicalWriteInfo.schema(),
                SaveMode.Append,
                this.dataSourceHelper.isDirectWrite(props.get("writePath")),
                logicalWriteInfo,
                this.bigQueryClient,
                this.config));

    BigQueryWriteBuilder writer = injector.getInstance(BigQueryWriteBuilder.class);
    return writer;
  }
}
