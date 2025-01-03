/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.direct;

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.friendlyTableName;

import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.LoggingBigQueryTracerFactory;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.InjectorBuilder;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.inject.Injector;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

public class DirectBigQueryRelationUtils {

  public static DirectBigQueryRelation createDirectBigQueryRelation(
      SQLContext sqlContext,
      Map<String, String> options,
      Optional<StructType> schema,
      DataSourceVersion dataSourceVersion) {
    InjectorBuilder injectorBuilder =
        new InjectorBuilder()
            .withSpark(sqlContext.sparkSession())
            .withOptions(options)
            .withDataSourceVersion(dataSourceVersion);
    schema.ifPresent(injectorBuilder::withSchema);
    Injector injector = injectorBuilder.build();

    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    TableInfo tableInfo = bigQueryClient.getReadTable(config.toReadTableOptions());
    String tableName = friendlyTableName(config.getTableId());
    if (tableInfo == null) {
      throw new IllegalArgumentException("Table " + tableName + " not found");
    }
    TableDefinition.Type tableType = tableInfo.getDefinition().getType();
    if (tableType.equals(TableDefinition.Type.TABLE)
        || tableType.equals(TableDefinition.Type.EXTERNAL)
        || tableType.equals(TableDefinition.Type.SNAPSHOT)) {
      return new DirectBigQueryRelation(
          config,
          tableInfo,
          bigQueryClient,
          injector.getInstance(BigQueryClientFactory.class),
          injector.getInstance(LoggingBigQueryTracerFactory.class),
          sqlContext);
    } else if (tableType.equals(TableDefinition.Type.VIEW)
        || tableType.equals(TableDefinition.Type.MATERIALIZED_VIEW)) {
      if (config.isViewsEnabled()) {
        return new DirectBigQueryRelation(
            config,
            tableInfo,
            bigQueryClient,
            injector.getInstance(BigQueryClientFactory.class),
            injector.getInstance(LoggingBigQueryTracerFactory.class),
            sqlContext);
      } else {
        throw new RuntimeException(
            "Views were not enabled. You can enable views by setting "
                + "'"
                + SparkBigQueryConfig.VIEWS_ENABLED_OPTION
                + "' to true. "
                + "Notice additional cost may occur.");
      }
    } else {
      throw new UnsupportedOperationException(
          "The type of table " + tableName + " is currently not supported: " + tableType);
    }
  }
}
