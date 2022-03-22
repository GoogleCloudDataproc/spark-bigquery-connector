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

import java.sql.{Date, Timestamp}
import java.util.{Optional, UUID}
import java.util.concurrent.{Callable, TimeUnit}
import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.Credentials
import com.google.cloud.bigquery.connector.common.{BigQueryClient, BigQueryClientFactory, BigQueryProxyTransporterBuilder, BigQueryUtil, ReadSessionCreator}
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
import com.google.cloud.bigquery.storage.v1.{ArrowSerializationOptions, BigQueryReadClient, BigQueryReadSettings, CreateReadSessionRequest, DataFormat, ReadSession}
import com.google.cloud.bigquery.{BigQuery, JobInfo, QueryJobConfiguration, Schema, StandardTableDefinition, TableDefinition, TableId, TableInfo}
import com.google.cloud.spark.bigquery.{BigQueryRelation, ScalaUtil, SchemaConverters, SparkBigQueryConfig, SparkBigQueryConnectorUserAgentProvider, SparkFilterUtils}
import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.collect.ImmutableList
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

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
    logInfo(
      s"""
         |Querying table $tableName, parameters sent from Spark:
         |requiredColumns=[${requiredColumns.mkString(",")}],
         |filters=[${filters.map(_.toString).mkString(",")}]"""
        .stripMargin.replace('\n', ' ').trim)
    val filter = getCompiledFilter(filters)

    val readSessionCreator = new ReadSessionCreator(options.toReadSessionCreatorConfig, bigQueryClient, bigQueryReadClientFactory)

    if (options.isOptimizedEmptyProjection && requiredColumns.isEmpty) {
      val actualTable = readSessionCreator.getActualTable(table, ImmutableList.copyOf(requiredColumns), SparkFilterUtils.emptyIfNeeded(filter))
      generateEmptyRowRDD(actualTable, if (readSessionCreator.isInputTableAView(table)) "" else filter)
    } else {
      if (requiredColumns.isEmpty) {
        logDebug(s"Not using optimized empty projection")
      }

      val readSessionResponse = readSessionCreator.create(tableId, ImmutableList.copyOf(requiredColumns), SparkFilterUtils.emptyIfNeeded(filter))
      val readSession = readSessionResponse.getReadSession
      val actualTable = readSessionResponse.getReadTableInfo

      val partitions = readSession.getStreamsList.asScala.map(_.getName)
        .zipWithIndex.map { case (name, i) => BigQueryPartition(name, i) }
        .toArray

      logInfo(s"Created read session for table '$tableName': ${readSession.getName}")

      val maxNumPartitionsRequested = getMaxNumPartitionsRequested(actualTable.getDefinition[TableDefinition])
      // This is spammy, but it will make it clear to users the number of partitions they got and
      // why.
      if (!maxNumPartitionsRequested.equals(partitions.length)) {
        logInfo(
          s"""Requested $maxNumPartitionsRequested max partitions, but only
             |received ${partitions.length} from the BigQuery Storage API for
             |session ${readSession.getName}. Notice that the number of streams in
             |actual may be lower than the requested number, depending on the
             |amount parallelism that is reasonable for the table and the
             |maximum amount of parallelism allowed by the system."""
            .stripMargin.replace('\n', ' '))
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
    logInfo(s"Used optimized BQ count(*) path. Count: $numberOfRows")
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
        ScalaUtil.noneIfEmpty(DirectBigQueryRelation.compileFilters(
          handledFilters(filters)))
      )
        .flatten
        .map(f => s"($f)")
        .mkString(" AND ")
    } else {
      // old behaviour, kept for backward compatibility
      // If a manual filter has been specified do not push down anything.
      options.getFilter.orElse {
        // TODO(pclay): Figure out why there are unhandled filters after we already listed them
        DirectBigQueryRelation.compileFilters(handledFilters(filters))
      }
    }
  }

  private def handledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(filter => DirectBigQueryRelation.isTopLevelFieldFilterHandled(
      options.getPushAllFilters, filter, options.getReadDataFormat, topLevelFields))
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    // If a manual filter has been specified tell Spark they are all unhandled
    if (options.getFilter.isPresent) {
      return filters
    }

    val unhandled = filters.filterNot(handledFilters(filters).contains)
    logDebug(s"unhandledFilters: ${unhandled.mkString(" ")}")
    unhandled
  }
}

object DirectBigQueryRelation {

  // used for testing
  var emptyRowRDDsCreated = 0;

  def isTopLevelFieldFilterHandled(
      pushAllFilters: Boolean,
      filter: Filter,
      readDataFormat: DataFormat,
      fields: Map[String, StructField]): Boolean = {
    if (pushAllFilters) {
      return true
    }
    filter match {
      case EqualTo(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case GreaterThan(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case GreaterThanOrEqual(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case LessThan(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case LessThanOrEqual(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case In(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case IsNull(attr) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case IsNotNull(attr) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case And(lhs, rhs) =>
        isTopLevelFieldFilterHandled(pushAllFilters, lhs, readDataFormat, fields) &&
        isTopLevelFieldFilterHandled(pushAllFilters, rhs, readDataFormat, fields)
      case Or(lhs, rhs) =>
        readDataFormat == DataFormat.AVRO &&
        isTopLevelFieldFilterHandled(pushAllFilters, lhs, readDataFormat, fields) &&
        isTopLevelFieldFilterHandled(pushAllFilters, rhs, readDataFormat, fields)
      case Not(child) => isTopLevelFieldFilterHandled(pushAllFilters, child, readDataFormat, fields)
      case StringStartsWith(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case StringEndsWith(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case StringContains(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case _ => false
    }
  }

  // BigQuery Storage API does not handle RECORD/STRUCT fields at the moment
  def isFilterWithNamedFieldHandled(
      filter: Filter,
      readDataFormat: DataFormat,
      fields: Map[String, StructField],
      fieldName: String): Boolean = {
    fields.get(fieldName)
      .filter(f => (f.dataType.isInstanceOf[StructType] || f.dataType.isInstanceOf[ArrayType]))
      .map(_ => false)
      .getOrElse(isHandled(filter, readDataFormat))
  }

  def isHandled(filter: Filter, readDataFormat: DataFormat): Boolean = filter match {
    case EqualTo(_, _) => true
    // There is no direct equivalent of EqualNullSafe in Google standard SQL.
    case EqualNullSafe(_, _) => false
    case GreaterThan(_, _) => true
    case GreaterThanOrEqual(_, _) => true
    case LessThan(_, _) => true
    case LessThanOrEqual(_, _) => true
    case In(_, _) => true
    case IsNull(_) => true
    case IsNotNull(_) => true
    case And(lhs, rhs) => isHandled(lhs, readDataFormat) && isHandled(rhs, readDataFormat)
    case Or(lhs, rhs) => readDataFormat == DataFormat.AVRO &&
      isHandled(lhs, readDataFormat) && isHandled(rhs, readDataFormat)
    case Not(child) => isHandled(child, readDataFormat)
    case StringStartsWith(_, _) => true
    case StringEndsWith(_, _) => true
    case StringContains(_, _) => true
    case _ => false
  }

  // Mostly copied from JDBCRDD.scala
  def compileFilter(filter: Filter): String = filter match {
    case EqualTo(attr, value) => s"${quote(attr)} = ${compileValue(value)}"
    case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
    case GreaterThanOrEqual(attr, value) => s"${quote(attr)} >= ${compileValue(value)}"
    case LessThan(attr, value) => s"${quote(attr)} < ${compileValue(value)}"
    case LessThanOrEqual(attr, value) => s"${quote(attr)} <= ${compileValue(value)}"
    case In(attr, values) => s"${quote(attr)} IN UNNEST(${compileValue(values)})"
    case IsNull(attr) => s"${quote(attr)} IS NULL"
    case IsNotNull(attr) => s"${quote(attr)} IS NOT NULL"
    case And(lhs, rhs) => Seq(lhs, rhs).map(compileFilter).map(p => s"($p)").mkString("(", " AND ", ")")
    case Or(lhs, rhs) => Seq(lhs, rhs).map(compileFilter).map(p => s"($p)").mkString("(", " OR ", ")")
    case Not(child) => Seq(child).map(compileFilter).map(p => s"(NOT ($p))").mkString
    case StringStartsWith(attr, value) =>
      s"${quote(attr)} LIKE '''${value.replace("'", "\\'")}%'''"
    case StringEndsWith(attr, value) =>
      s"${quote(attr)} LIKE '''%${value.replace("'", "\\'")}'''"
    case StringContains(attr, value) =>
      s"${quote(attr)} LIKE '''%${value.replace("'", "\\'")}%'''"
    case _ => throw new IllegalArgumentException(s"Invalid filter: $filter")
  }

  def compileFilters(filters: Iterable[Filter]): String = {
    filters.map(compileFilter).toSeq.sorted.mkString(" AND ")
  }

  /**
   * Converts value to SQL expression.
   */
  private def compileValue(value: Any): Any = value match {
    case null => "null"
    case stringValue: String => s"'${stringValue.replace("'", "\\'")}'"
    case timestampValue: Timestamp => "TIMESTAMP '" + timestampValue + "'"
    case dateValue: Date => "DATE '" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString("[", ", ", "]")
    case _ => value
  }

  private def quote(attr: String): String = {
    s"""`$attr`"""
  }

  def toSqlTableReference(tableId: TableId): String =
    s"${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}"
}
