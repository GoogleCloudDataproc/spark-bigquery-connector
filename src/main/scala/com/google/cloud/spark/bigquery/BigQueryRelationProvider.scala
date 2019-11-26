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
import com.google.cloud.bigquery.TableDefinition.Type.{TABLE, VIEW}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, TableDefinition}
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class BigQueryRelationProvider(
    getBigQuery: () => Option[BigQuery])
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
                                    schema)
    val bigquery =
      getBigQuery().getOrElse(BigQueryRelationProvider.createBigQuery(opts))
    val tableName = BigQueryUtil.friendlyTableName(opts.tableId)
    // TODO(#7): Support creating non-existent tables with write support.
    val table = Option(bigquery.getTable(opts.tableId))
      .getOrElse(sys.error(s"Table $tableName not found"))
    table.getDefinition[TableDefinition].getType match {
      case TABLE => new DirectBigQueryRelation(opts, table)(sqlContext)
      case VIEW => if (opts.viewsEnabled) {
        new DirectBigQueryRelation(opts, table)(sqlContext)
      } else {
        sys.error(
          s"""Views were not enabled. You can enable views by setting
             |'${SparkBigQueryOptions.ViewsEnabledOption}' to true.
             |Notice additional cost may occur."""
            .stripMargin.replace('\n', ' '))
      }
    }
  }

  // to allow write support
  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               data: DataFrame): BaseRelation = {
    val options = createSparkBigQueryOptions(sqlContext, parameters, Option(data.schema))
    val bigQuery = getOrCreateBigQuery(options)
    val relation = BigQueryInsertableRelation(bigQuery, sqlContext, options)

    mode match {
      case SaveMode.Append => relation.insert(data, overwrite = false)
      case SaveMode.Overwrite => relation.insert(data, overwrite = true)
      case SaveMode.ErrorIfExists =>
        if (!relation.exists) {
          relation.insert(data, overwrite = false)
        } else {
          throw new IllegalArgumentException(
            s"""SaveMode is set to ErrorIfExists and Table
               |${BigQueryUtil.friendlyTableName(options.tableId)}
               |already exists. Did you want to add data to the table by setting
               |the SaveMode to Append? Example:
               |df.write.format.options.mode(SaveMode.Append).save()"""
              .stripMargin.replace('\n', ' '))
        }
      case SaveMode.Ignore =>
        if (!relation.exists) {
          relation.insert(data, overwrite = false)
        }
    }

    relation
  }

  private def getOrCreateBigQuery(options: SparkBigQueryOptions) =
    getBigQuery().getOrElse(BigQueryRelationProvider.createBigQuery(options))

  def createSparkBigQueryOptions(sqlContext: SQLContext,
                                 parameters: Map[String, String],
                                 schema: Option[StructType] = None): SparkBigQueryOptions = {
    SparkBigQueryOptions(parameters,
      sqlContext.getAllConfs,
      sqlContext.sparkContext.hadoopConfiguration,
      schema,
      defaultParentProject)
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
