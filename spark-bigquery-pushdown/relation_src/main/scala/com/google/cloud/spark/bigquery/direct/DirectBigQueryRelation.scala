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
package com.google.cloud.spark.bigquery.direct

import com.google.cloud.bigquery._
import com.google.cloud.bigquery.connector.common.{BigQueryClient, BigQueryClientFactory, BigQueryUtil, ReadSessionCreator}
import com.google.cloud.spark.bigquery._
import com.google.common.collect.ImmutableList
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.JavaConverters._

private[bigquery] class DirectBigQueryRelation(
    options: SparkBigQueryConfig,
    table: TableInfo,
    bigQueryClient: BigQueryClient,
    bigQueryReadClientFactory: BigQueryClientFactory)
    (@transient override val sqlContext: SQLContext)
  extends BigQueryRelation(options, table)(sqlContext)
    with TableScan with PrunedScan with PrunedFilteredScan {

  val topLevelFields = SchemaConverters
    .toSpark(SchemaConverters.getSchemaWithPseudoColumns(table))
    .fields
    .map(field => (field.name, field))
    .toMap

  val bigQueryRDDFactory = new BigQueryRDDFactory(bigQueryClient, bigQueryReadClientFactory, options, sqlContext)

  override val needConversion: Boolean = false
  override val sizeInBytes: Long = bigQueryRDDFactory.getNumBytes(defaultTableDefinition)
  // no added filters and with all column
  lazy val defaultTableDefinition: TableDefinition = table.getDefinition[TableDefinition]

  override def buildScan(): RDD[Row] = {
    buildScan(schema.fieldNames)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    buildScan(requiredColumns, Array())
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logInfo(
      s"""
         |Querying table $tableName, parameters sent from Spark:
         |requiredColumns=[${requiredColumns.mkString(",")}],
         |filters=[${filters.map(_.toString).mkString(",")}]"""
        .stripMargin.replace('\n', ' ').trim)
    val filter = getCompiledFilter(filters)

    val readSessionCreator = new ReadSessionCreator(options.toReadSessionCreatorConfig, bigQueryClient, bigQueryReadClientFactory)

    if (options.isOptimizedEmptyProjection && requiredColumns.isEmpty) {
      val actualTable = readSessionCreator.getActualTable(table, ImmutableList.copyOf(requiredColumns), BigQueryUtil.emptyIfNeeded(filter))
      generateEmptyRowRDD(actualTable, if (readSessionCreator.isInputTableAView(table)) "" else filter)
    } else {
      if (requiredColumns.isEmpty) {
        logDebug(s"Not using optimized empty projection")
      }

      bigQueryRDDFactory.createRddFromTable(tableId, readSessionCreator, requiredColumns, filter).asInstanceOf[RDD[Row]]
    }
  }

  def generateEmptyRowRDD(tableInfo: TableInfo, filter: String) : RDD[Row] = {
    DirectBigQueryRelation.emptyRowRDDsCreated += 1
    val numberOfRows: Long = if (filter.length == 0) {
      // no need to query the table
      tableInfo.getNumRows.longValue
    } else {
      // run a query
      val table = DirectBigQueryRelation.toSqlTableReference(tableInfo.getTableId)
      val sql = s"SELECT COUNT(*) from `$table` WHERE $filter"
      val result = bigQueryClient.query(sql)
      result.iterateAll.iterator.next.get(0).getLongValue
    }
    logInfo(s"Used optimized BQ count(*) path. Count: $numberOfRows")
    sqlContext.sparkContext.range(0, numberOfRows)
      .map(_ => InternalRow.empty)
      .asInstanceOf[RDD[Row]]
  }

  // VisibleForTesting
  private[bigquery] def getCompiledFilter(filters: Array[Filter]): String = {
    if (options.isCombinePushedDownFilters) {
      // new behaviour, fixing
      // https://github.com/GoogleCloudPlatform/spark-bigquery-connector/issues/74
      Seq(
        ScalaUtil.toOption(options.getFilter),
        ScalaUtil.noneIfEmpty(SparkFilterUtils.compileFilters(
          SparkFilterUtils.handledFilters(options.getPushAllFilters, options.getReadDataFormat, ImmutableList.copyOf(filters))))
      )
        .flatten
        .map(f => s"($f)")
        .mkString(" AND ")
    } else {
      // old behaviour, kept for backward compatibility
      // If a manual filter has been specified do not push down anything.
      options.getFilter.orElse {
        // TODO(pclay): Figure out why there are unhandled filters after we already listed them
        SparkFilterUtils.compileFilters(SparkFilterUtils.handledFilters(options.getPushAllFilters, options.getReadDataFormat, ImmutableList.copyOf(filters)))
      }
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    // If a manual filter has been specified tell Spark they are all unhandled
    if (options.getFilter.isPresent) {
      return filters
    }

    val unhandled = SparkFilterUtils.unhandledFilters(options.getPushAllFilters, options.getReadDataFormat, ImmutableList.copyOf(filters)).toArray
    logDebug(s"unhandledFilters: ${unhandled.mkString(" ")}")
    unhandled
  }
}

object DirectBigQueryRelation {

  // used for testing
  var emptyRowRDDsCreated = 0

  def toSqlTableReference(tableId: TableId): String =
    s"${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}"
}
