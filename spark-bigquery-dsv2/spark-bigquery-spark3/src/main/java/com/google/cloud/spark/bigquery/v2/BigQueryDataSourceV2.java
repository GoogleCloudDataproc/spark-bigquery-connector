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

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.common.BigQueryDataSourceHelper;
import com.google.inject.Injector;
import java.util.*;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class BigQueryDataSourceV2 implements TableProvider, DataSourceRegister {

  private BigQueryDataSourceHelper dataSourceHelper = new BigQueryDataSourceHelper();
  CaseInsensitiveStringMap options;

  @Override
  public StructType inferSchema(final CaseInsensitiveStringMap options) {
    this.options = options;
    if (options.get("schema") != null) {
      return getTable(StructType.fromDDL(options.get("schema")), null, options.asCaseSensitiveMap())
          .schema();
    }
    StructField[] structFields = new StructField[0];
    return getTable(new StructType(structFields), null, options).schema();
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    Map<String, String> props = new HashMap<>(properties);
    Injector injector;
    injector =
        this.dataSourceHelper.createInjector(
            schema, props, false, null, new BigQueryTableModule(schema, props));
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    TableInfo table = bigQueryClient.getTable(config.getTableId());
    if (table != null) {
      schema = this.dataSourceHelper.getSchema();
      injector =
          this.dataSourceHelper.createInjector(
              schema, props, false, null, new BigQueryTableModule(schema, props));
    }
    return injector.getInstance(BigQueryTable.class);
  }

  @Override
  public String shortName() {
    return "bigquery";
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }
}
