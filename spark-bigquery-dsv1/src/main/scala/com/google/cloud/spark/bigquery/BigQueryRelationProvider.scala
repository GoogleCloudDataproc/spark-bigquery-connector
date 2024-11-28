/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery

import java.util.Optional
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TableDefinition.Type.{EXTERNAL, MATERIALIZED_VIEW, SNAPSHOT, TABLE, VIEW}
import com.google.cloud.bigquery.connector.common.{BigQueryClient, BigQueryClientFactory, BigQueryClientModule, BigQueryUtil, LoggingBigQueryTracerFactory}
import com.google.cloud.spark.bigquery.direct.{DirectBigQueryRelation, DirectBigQueryRelationUtils}
import com.google.cloud.spark.bigquery.write.CreatableRelationProviderHelper
import com.google.common.collect.ImmutableMap
import com.google.inject.{Guice, Injector}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.JavaConverters._

class BigQueryRelationProvider(
                                getGuiceInjectorCreator: () => GuiceInjectorCreator)
  extends RelationProvider
    with CreatableRelationProvider
    with SchemaRelationProvider
    with DataSourceRegister
    with StreamSinkProvider {

  BigQueryUtilScala.validateScalaVersionCompatibility

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    createRelationInternal(sqlContext, parameters)
  }

  def this() = this(() => new GuiceInjectorCreator {})

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    createRelationInternal(sqlContext, parameters, schema = Some(schema))
  }

  // To allow streaming writes
  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode): Sink = {
    val injector = getGuiceInjectorCreator().createGuiceInjector(sqlContext, parameters)
    val opts = injector.getInstance(classOf[SparkBigQueryConfig])
    val bigQueryClient = injector.getInstance(classOf[BigQueryClient])
    BigQueryStreamingSink(
      sqlContext, parameters, partitionColumns, outputMode, opts, bigQueryClient)
  }

  protected def createRelationInternal(
                                        sqlContext: SQLContext,
                                        parameters: Map[String, String],
                                        schema: Option[StructType] = None): BigQueryRelation = {
    val schemaOptional = Optional.ofNullable(schema.orNull)
    DirectBigQueryRelationUtils.createDirectBigQueryRelation(
      sqlContext, parameters.asJava, schemaOptional, DataSourceVersion.V1)
  }

  // to allow write support
  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               data: DataFrame): BaseRelation = {
    val customDefaults = ImmutableMap.of[String, String]()
    new CreatableRelationProviderHelper()
      .createRelation(sqlContext, mode, parameters, data, customDefaults)
  }

  // This method is kept here as OpenLineage uses it.
  def createSparkBigQueryConfig(sqlContext: SQLContext,
                                parameters: Map[String, String],
                                schema: Option[StructType] = None): SparkBigQueryConfig = {
    SparkBigQueryUtil.createSparkBigQueryConfig(
      sqlContext, parameters, schema, DataSourceVersion.V1)
  }

  override def shortName: String = "bigquery"
}

// externalized to be used by tests
trait GuiceInjectorCreator {
  def createGuiceInjector(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          schema: Option[StructType] = None): Injector = {
    val spark = sqlContext.sparkSession
    val injector = Guice.createInjector(
      new BigQueryClientModule,
      new SparkBigQueryConnectorModule(
        spark,
        parameters.asJava,
        Map.empty[String, String].asJava,
        Optional.ofNullable(schema.orNull),
        DataSourceVersion.V1,
        /* tableIsMandatory */ true,
        java.util.Optional.empty()))
    injector
  }
}

// DefaultSource is required for spark.read.format("com.google.cloud.spark.bigquery")
// BigQueryRelationProvider is more consistent with the internal providers
class DefaultSource extends BigQueryRelationProvider
