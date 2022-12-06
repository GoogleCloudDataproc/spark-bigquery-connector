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
import com.google.cloud.bigquery.TableDefinition.Type.{EXTERNAL, MATERIALIZED_VIEW, TABLE, VIEW}
import com.google.cloud.bigquery.connector.common.{BigQueryClient, BigQueryClientFactory, BigQueryClientModule, LoggingBigQueryTracerFactory, BigQueryUtil}
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
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
    val injector = getGuiceInjectorCreator().createGuiceInjector(sqlContext, parameters, schema)
    val opts = injector.getInstance(classOf[SparkBigQueryConfig])
    val bigQueryClient = injector.getInstance(classOf[BigQueryClient])
    val tableInfo = bigQueryClient.getReadTable(opts.toReadTableOptions)
    val tableName = BigQueryUtil.friendlyTableName(opts.getTableId)
    val bigQueryReadClientFactory = injector.getInstance(classOf[BigQueryClientFactory])
    val bigQueryTracerFactory = injector.getInstance(classOf[LoggingBigQueryTracerFactory])
    val table = Option(tableInfo)
      .getOrElse(sys.error(s"Table $tableName not found"))
    table.getDefinition[TableDefinition].getType match {
      case TABLE | EXTERNAL => new DirectBigQueryRelation(opts, table, bigQueryClient, bigQueryReadClientFactory, bigQueryTracerFactory, sqlContext)
      case VIEW | MATERIALIZED_VIEW => if (opts.isViewsEnabled) {
        new DirectBigQueryRelation(opts, table, bigQueryClient, bigQueryReadClientFactory, bigQueryTracerFactory, sqlContext)
      } else {
        sys.error(
          s"""Views were not enabled. You can enable views by setting
             |'${SparkBigQueryConfig.VIEWS_ENABLED_OPTION}' to true.
             |Notice additional cost may occur."""
            .stripMargin.replace('\n', ' '))
      }
      case unsupported => throw new UnsupportedOperationException(
        s"The type of table $tableName is currently not supported: $unsupported")
    }
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

  def createSparkBigQueryConfig(sqlContext: SQLContext,
                                parameters: Map[String, String],
                                schema: Option[StructType] = None): SparkBigQueryConfig = {
    SparkBigQueryConfig.from(parameters.asJava,
      ImmutableMap.of[String, String](),
      DataSourceVersion.V1,
      sqlContext.sparkSession,
      Optional.ofNullable(schema.orNull),
      /* tableIsMandatory */ true)
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
