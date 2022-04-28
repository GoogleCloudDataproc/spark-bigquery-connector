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
    extends BigQueryRelation(options, table, sqlContext)
        with TableScan with PrunedScan with PrunedFilteredScan {

  val topLevelFields = SchemaConverters
    .toSpark(SchemaConverters.getSchemaWithPseudoColumns(table))
    .fields
    .map(field => (field.name, field))
    .toMap

  /**
   * Default parallelism to 1 reader per 400MB, which should be about the maximum allowed by the
   * BigQuery Storage API. The number of partitions returned may be significantly less depending
   * on a number of factors.
   */
  val DEFAULT_BYTES_PER_PARTITION = 400L * 1000 * 1000

  override val needConversion: Boolean = false
  override val sizeInBytes: Long = getNumBytes(defaultTableDefinition)
  // no added filters and with all column
  lazy val defaultTableDefinition: TableDefinition = table.getDefinition[TableDefinition]

  override def buildScan(): RDD[Row] = {
    buildScan(schema.fieldNames)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    buildScan(requiredColumns, Array())
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
//    logInfo(
//      s"""
//         |Querying table $tableName, parameters sent from Spark:
//         |requiredColumns=[${requiredColumns.mkString(",")}],
//         |filters=[${filters.map(_.toString).mkString(",")}]"""
//        .stripMargin.replace('\n', ' ').trim)
    val filter = getCompiledFilter(filters)

    val readSessionCreator = new ReadSessionCreator(options.toReadSessionCreatorConfig, bigQueryClient, bigQueryReadClientFactory)

    if (options.isOptimizedEmptyProjection && requiredColumns.isEmpty) {
      val actualTable = readSessionCreator.getActualTable(table, ImmutableList.copyOf(requiredColumns), BigQueryUtil.emptyIfNeeded(filter))
      generateEmptyRowRDD(actualTable, if (readSessionCreator.isInputTableAView(table)) "" else filter)
    } else {
      if (requiredColumns.isEmpty) {
        //logDebug(s"Not using optimized empty projection")
      }

      val readSessionResponse = readSessionCreator.create(getTableId(), ImmutableList.copyOf(requiredColumns), BigQueryUtil.emptyIfNeeded(filter))
      val readSession = readSessionResponse.getReadSession
      val actualTable = readSessionResponse.getReadTableInfo

      val partitions = readSession.getStreamsList.asScala.map(_.getName)
        .zipWithIndex.map { case (name, i) => new BigQueryPartition(name, i) }
        .toArray

      //logInfo(s"Created read session for table '$tableName': ${readSession.getName}")

      val maxNumPartitionsRequested = getMaxNumPartitionsRequested(actualTable.getDefinition[TableDefinition])
      // This is spammy, but it will make it clear to users the number of partitions they got and
      // why.
      if (!maxNumPartitionsRequested.equals(partitions.length)) {
//        logInfo(
//          s"""Requested $maxNumPartitionsRequested max partitions, but only
//             |received ${partitions.length} from the BigQuery Storage API for
//             |session ${readSession.getName}. Notice that the number of streams in
//             |actual may be lower than the requested number, depending on the
//             |amount parallelism that is reasonable for the table and the
//             |maximum amount of parallelism allowed by the system."""
//            .stripMargin.replace('\n', ' '))
      }

      val requiredColumnSet = requiredColumns.toSet
      val prunedSchema = Schema.of(
        SchemaConverters.getSchemaWithPseudoColumns(actualTable).getFields().asScala
          .filter(f => requiredColumnSet.contains(f.getName)).asJava)

      BigQueryRDD.scanTable(
        sqlContext,
        partitions.asInstanceOf[Array[Partition]],
        readSession,
        prunedSchema,
        requiredColumns,
        options,
        bigQueryReadClientFactory).asInstanceOf[RDD[Row]]
    }
  }

  def  generateEmptyRowRDD(tableInfo: TableInfo, filter: String) : RDD[Row] = {
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
    //logInfo(s"Used optimized BQ count(*) path. Count: $numberOfRows")
    sqlContext.sparkContext.range(0, numberOfRows)
      .map(_ => InternalRow.empty)
      .asInstanceOf[RDD[Row]]
  }

  /**
   * The theoretical number of Partitions of the returned DataFrame.
   * If the table is small the server will provide fewer readers and there will be fewer
   * partitions.
   *
   * VisibleForTesting
   */
  def getMaxNumPartitionsRequested: Int =
    getMaxNumPartitionsRequested(defaultTableDefinition)

  def getMaxNumPartitionsRequested(tableDefinition: TableDefinition): Int =
    options.getMaxParallelism
      .orElse(Math.max(
        (getNumBytes(tableDefinition) / DEFAULT_BYTES_PER_PARTITION).toInt, 1))

  def getNumBytes(tableDefinition: TableDefinition): Long = {
    val tableType = tableDefinition.getType
    if (TableDefinition.Type.EXTERNAL == tableType ||
      (options.isViewsEnabled &&
        (TableDefinition.Type.VIEW == tableType ||
        TableDefinition.Type.MATERIALIZED_VIEW == tableType))) {
      sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes
    } else {
      tableDefinition.asInstanceOf[StandardTableDefinition].getNumBytes
    }
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
    //logDebug(s"unhandledFilters: ${unhandled.mkString(" ")}")
    unhandled
  }
}

object DirectBigQueryRelation {

  // used for testing
  var emptyRowRDDsCreated = 0

  def toSqlTableReference(tableId: TableId): String =
    s"${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}"
}
