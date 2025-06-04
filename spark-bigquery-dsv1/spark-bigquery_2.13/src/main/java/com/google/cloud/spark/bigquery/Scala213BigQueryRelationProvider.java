/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import java.util.function.Supplier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.sources.StreamSinkProvider;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

public class Scala213BigQueryRelationProvider extends BigQueryRelationProviderBase
    implements RelationProvider,
        CreatableRelationProvider,
        SchemaRelationProvider,
        StreamSinkProvider {

  public Scala213BigQueryRelationProvider() {
    super();
  }

  public Scala213BigQueryRelationProvider(Supplier<GuiceInjectorCreator> getGuiceInjectorCreator) {
    super(getGuiceInjectorCreator);
  }

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
    return super.createRelation(sqlContext, mode, CollectionConverters.asJava(parameters), data);
  }

  @Override
  public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
    return super.createRelation(sqlContext, CollectionConverters.asJava(parameters));
  }

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext, Map<String, String> parameters, StructType schema) {
    return super.createRelation(sqlContext, CollectionConverters.asJava(parameters), schema);
  }

  @Override
  public Sink createSink(
      SQLContext sqlContext,
      Map<String, String> parameters,
      Seq<String> partitionColumns,
      OutputMode outputMode) {
    return super.createSink(
        sqlContext,
        CollectionConverters.asJava(parameters),
        CollectionConverters.asJava(partitionColumns),
        outputMode);
  }
}
