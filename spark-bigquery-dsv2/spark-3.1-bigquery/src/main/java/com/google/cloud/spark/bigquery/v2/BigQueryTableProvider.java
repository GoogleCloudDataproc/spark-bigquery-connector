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

import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.spark.bigquery.InjectorFactory;
import com.google.cloud.spark.bigquery.write.CreatableRelationProviderHelper;
import com.google.inject.Injector;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class BigQueryTableProvider extends BaseBigQuerySource
    implements TableProvider, CreatableRelationProvider {

  private static final String DEFAULT_CATALOG_NAME = "bigquery";
  private static final String DEFAULT_CATALOG = "spark.sql.catalog." + DEFAULT_CATALOG_NAME;
  private static final Transform[] EMPTY_TRANSFORM_ARRAY = {};

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return getBigQueryTableInternal(options).schema();
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    setupDefaultSparkCatalog(SparkSession.active());
    return getBigQueryTableInternal(schema, properties);
  }

  private BigQueryTable getBigQueryTableInternal(Map<String, String> properties) {
    return getBigQueryTableInternal(null, properties);
  }

  private BigQueryTable getBigQueryTableInternal(
      StructType schema, Map<String, String> properties) {
    try {
      Injector injector =
          InjectorFactory.createInjector(schema, properties, /* tableIsMandatory */ true);
      BigQueryTable table = BigQueryTable.fromConfigurationAndSchema(injector, schema);
      return table;
    } catch (NoSuchTableException e) {
      throw new BigQueryConnectorException("Table was not found", e);
    }
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  private static void setupDefaultSparkCatalog(SparkSession spark) {
    if (spark.conf().contains(DEFAULT_CATALOG)) {
      return;
    }
    spark.conf().set(DEFAULT_CATALOG, BigQueryCatalog.class.getCanonicalName());
  }

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext,
      SaveMode mode,
      scala.collection.immutable.Map<String, String> parameters,
      Dataset<Row> data) {
    return new CreatableRelationProviderHelper().createRelation(sqlContext, mode, parameters, data);
  }
}
