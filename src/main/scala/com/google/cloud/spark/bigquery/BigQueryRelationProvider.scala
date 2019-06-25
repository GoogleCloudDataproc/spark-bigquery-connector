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

import com.google.auth.Credentials
import com.google.cloud.bigquery.TableDefinition.Type.TABLE
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, TableDefinition}
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

class BigQueryRelationProvider(
    getBigQuery: () => Option[BigQuery],
    // This should never be nullable, but could be in very strange circumstances
    defaultParentProject: Option[String] = Option(
      BigQueryOptions.getDefaultInstance.getProjectId))
    extends RelationProvider
    with SchemaRelationProvider
    with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    createRelationInternal(sqlContext, parameters)
  }

  def this() = this(() => None)

  protected def createRelationInternal(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: Option[StructType] = None): BigQueryRelation = {
    val opts = SparkBigQueryOptions(parameters,
                                    sqlContext.getAllConfs,
                                    sqlContext.sparkContext.hadoopConfiguration,
                                    schema,
                                    defaultParentProject)
    val bigquery =
      getBigQuery().getOrElse(BigQueryRelationProvider.createBigQuery(opts))
    val tableName = BigQueryUtil.friendlyTableName(opts.tableId)
    // TODO(#7): Support creating non-existent tables with write support.
    val table = Option(bigquery.getTable(opts.tableId))
      .getOrElse(sys.error(s"Table $tableName not found"))
    table.getDefinition[TableDefinition].getType match {
      case TABLE => new DirectBigQueryRelation(opts, table)(sqlContext)
      case other => sys.error(s"Table type $other is not supported.")
    }
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    createRelationInternal(sqlContext, parameters, schema = Some(schema))
  }

  override def shortName: String = "bigquery"
}

object BigQueryRelationProvider {

  def createBigQuery(options: SparkBigQueryOptions): BigQuery =
    options.createCredentials.fold(
      BigQueryOptions.getDefaultInstance.getService
    )(bigQueryWithCredentials(options.parentProject, _))

  private def bigQueryWithCredentials(parentProject: String,
                                    credentials: Credentials): BigQuery = {
    BigQueryOptions
      .newBuilder()
      .setProjectId(parentProject)
      .setCredentials(credentials)
      .build()
      .getService
  }

}

// DefaultSource is required for spark.read.format("com.google.cloud.spark.bigquery")
// BigQueryRelationProvider is more consistent with the internal providers
class DefaultSource extends BigQueryRelationProvider
