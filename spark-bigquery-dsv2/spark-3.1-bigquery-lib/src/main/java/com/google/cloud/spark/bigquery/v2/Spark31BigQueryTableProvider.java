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

import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.InjectorBuilder;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelationUtils;
import com.google.cloud.spark.bigquery.write.CreatableRelationProviderHelper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.openlineage.spark.shade.client.OpenLineage;
import io.openlineage.spark.shade.client.utils.DatasetIdentifier;
import io.openlineage.spark.shade.extension.v1.LineageRelationProvider;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;

public class Spark31BigQueryTableProvider extends BaseBigQuerySource
    implements TableProvider, RelationProvider, CreatableRelationProvider, LineageRelationProvider {

  private static final Transform[] EMPTY_TRANSFORM_ARRAY = {};

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return getBigQueryTableInternal(options).schema();
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return Spark3Util.createBigQueryTableInstance(Spark31BigQueryTable::new, schema, properties);
  }

  protected Table getBigQueryTableInternal(Map<String, String> properties) {
    return Spark3Util.createBigQueryTableInstance(Spark31BigQueryTable::new, null, properties);
  }

  @Override
  public boolean supportsExternalMetadata() {
    return false;
  }

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext, scala.collection.immutable.Map<String, String> parameters) {
    return DirectBigQueryRelationUtils.createDirectBigQueryRelation(
        sqlContext,
        JavaConverters.mapAsJavaMap(parameters),
        /* schema */ Optional.empty(),
        DataSourceVersion.V1);
  }

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext,
      SaveMode mode,
      scala.collection.immutable.Map<String, String> parameters,
      Dataset<Row> data) {
    return new CreatableRelationProviderHelper()
        .createRelation(sqlContext, mode, parameters, data, ImmutableMap.of());
  }

  @Override
  public DatasetIdentifier getLineageDatasetIdentifier(
      String sparkListenerEventName,
      OpenLineage openLineage,
      Object sqlContext,
      Object parameters) {
    @SuppressWarnings("unchecked")
    Map<String, String> properties =
        JavaConverters.mapAsJavaMap((CaseInsensitiveMap<String>) parameters);
    Injector injector = new InjectorBuilder().withOptions(properties).build();
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    return new DatasetIdentifier(BigQueryUtil.friendlyTableName(config.getTableId()), "bigquery");
  }
}
