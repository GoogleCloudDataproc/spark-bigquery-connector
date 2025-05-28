/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.bigquery.connector.common.LoggingBigQueryTracerFactory;
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation;
import com.google.cloud.spark.bigquery.write.CreatableRelationProviderHelper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

public class BigQueryRelationProviderBase implements DataSourceRegister {

  private final Supplier<GuiceInjectorCreator> getGuiceInjectorCreator;

  public BigQueryRelationProviderBase(Supplier<GuiceInjectorCreator> getGuiceInjectorCreator) {
    this.getGuiceInjectorCreator = getGuiceInjectorCreator;
    BigQueryUtilScala.validateScalaVersionCompatibility();
  }

  public BigQueryRelationProviderBase() {
    this(() -> new GuiceInjectorCreator() {}); // Default creator
  }

  public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
    return createRelationInternal(sqlContext, parameters, Optional.empty());
  }

  public BaseRelation createRelation(
      SQLContext sqlContext, Map<String, String> parameters, StructType schema) {
    return createRelationInternal(sqlContext, parameters, Optional.of(schema));
  }

  public Sink createSink(
      SQLContext sqlContext,
      Map<String, String> parameters,
      List<String> partitionColumns, // Scala Seq
      OutputMode outputMode) {
    Injector injector =
        getGuiceInjectorCreator.get().createGuiceInjector(sqlContext, parameters, Optional.empty());
    SparkBigQueryConfig opts = injector.getInstance(SparkBigQueryConfig.class);
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    return new BigQueryStreamingSink(
        sqlContext, parameters, partitionColumns, outputMode, opts, bigQueryClient);
  }

  protected BigQueryRelation createRelationInternal(
      SQLContext sqlContext, Map<String, String> parameters, Optional<StructType> schema) {
    Injector injector =
        getGuiceInjectorCreator.get().createGuiceInjector(sqlContext, parameters, schema);
    SparkBigQueryConfig opts = injector.getInstance(SparkBigQueryConfig.class);
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    TableInfo tableInfo = bigQueryClient.getReadTable(opts.toReadTableOptions());
    String tableName = BigQueryUtil.friendlyTableName(opts.getTableId());
    BigQueryClientFactory bigQueryReadClientFactory =
        injector.getInstance(BigQueryClientFactory.class);
    LoggingBigQueryTracerFactory bigQueryTracerFactory =
        injector.getInstance(LoggingBigQueryTracerFactory.class);

    TableInfo table =
        Optional.ofNullable(tableInfo)
            .orElseThrow(() -> new RuntimeException("Table " + tableName + " not found"));

    TableDefinition.Type tableType = table.getDefinition().getType();

    if (tableType == TableDefinition.Type.TABLE
        || tableType == TableDefinition.Type.EXTERNAL
        || tableType == TableDefinition.Type.SNAPSHOT) {
      return new DirectBigQueryRelation(
          opts,
          table,
          bigQueryClient,
          bigQueryReadClientFactory,
          bigQueryTracerFactory,
          sqlContext);
    } else if (tableType == TableDefinition.Type.VIEW
        || tableType == TableDefinition.Type.MATERIALIZED_VIEW) {
      if (opts.isViewsEnabled()) {
        return new DirectBigQueryRelation(
            opts,
            table,
            bigQueryClient,
            bigQueryReadClientFactory,
            bigQueryTracerFactory,
            sqlContext);
      } else {
        throw new RuntimeException(
            String.format(
                "Views were not enabled. You can enable views by setting '%s' to true. "
                    + "Notice additional cost may occur.",
                SparkBigQueryConfig.VIEWS_ENABLED_OPTION));
      }
    } else {
      throw new UnsupportedOperationException(
          "The type of table " + tableName + " is currently not supported: " + tableType);
    }
  }

  public BaseRelation createRelation(
      SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
    ImmutableMap<String, String> customDefaults = ImmutableMap.of();
    return new CreatableRelationProviderHelper()
        .createRelation(sqlContext, mode, parameters, data, customDefaults);
  }

  public SparkBigQueryConfig createSparkBigQueryConfig(
      SQLContext sqlContext, Map<String, String> parameters, Optional<StructType> schema) {
    return SparkBigQueryUtil.createSparkBigQueryConfig(
        sqlContext, parameters, schema, DataSourceVersion.V1);
  }

  @Override
  public String shortName() {
    return "bigquery";
  }
}
