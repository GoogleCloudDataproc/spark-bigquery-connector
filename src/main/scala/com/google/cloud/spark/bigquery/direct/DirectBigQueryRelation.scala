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

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.Credentials
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions
import com.google.cloud.bigquery.storage.v1beta1.Storage.{CreateReadSessionRequest, DataFormat, ShardingStrategy}
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto.TableReference
import com.google.cloud.bigquery.storage.v1beta1.{BigQueryStorageClient, BigQueryStorageSettings}
import com.google.cloud.bigquery.{Schema, StandardTableDefinition, TableDefinition, TableInfo}
import com.google.cloud.spark.bigquery.{BigQueryRelation, BigQueryUtil, BuildInfo, SparkBigQueryOptions}
import com.typesafe.scalalogging.Logger
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._

import scala.collection.JavaConverters._

private[bigquery] class DirectBigQueryRelation(
    options: SparkBigQueryOptions,
    table: TableInfo, getClient: SparkBigQueryOptions => BigQueryStorageClient =
        DirectBigQueryRelation.createReadClient)
    (@transient override val sqlContext: SQLContext)
    extends BigQueryRelation(options, table)(sqlContext)
        with TableScan with PrunedScan with PrunedFilteredScan {

  val tableReference: TableReference = TableReference.newBuilder()
      .setProjectId(tableId.getProject)
      .setDatasetId(tableId.getDataset)
      .setTableId(tableId.getTable)
      .build()
  val tableDefinition: StandardTableDefinition = {
    require(TableDefinition.Type.TABLE == table.getDefinition[TableDefinition].getType)
    table.getDefinition[StandardTableDefinition]
  }
  private val log = Logger(getClass)

  /**
   * Default parallelism to 1 reader per 400MB, which should be about the maximum allowed by the
   * BigQuery Storage API. The number of partitions returned may be significantly less depending
   * on a number of factors.
   */
  val DEFAULT_BYTES_PER_PARTITION = 400L * 1000 * 1000

  override val needConversion: Boolean = false
  override val sizeInBytes: Long = tableDefinition.getNumBytes

  override def buildScan(): RDD[Row] = {
    buildScan(schema.fieldNames)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    buildScan(requiredColumns, Array())
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    log.debug(s"filters pushed: ${filters.mkString(", ")}")
    val filter = getCompiledFilter(filters)
    log.debug(s"buildScan: cols: [${requiredColumns.mkString(", ")}], filter: '$filter'")
    val readOptions = TableReadOptions.newBuilder()
        .addAllSelectedFields(requiredColumns.toList.asJava)
        .setRowRestriction(filter)
        .build()
    val requiredColumnSet = requiredColumns.toSet
    val prunedSchema = Schema.of(
      tableDefinition.getSchema.getFields.asScala
          .filter(f => requiredColumnSet.contains(f.getName)).asJava)

    val client = getClient(options)

    val numPartitionsRequested = getNumPartitionsRequested

    try {
      val session = client.createReadSession(
        CreateReadSessionRequest.newBuilder()
            .setParent(s"projects/${options.parentProject}")
            .setFormat(DataFormat.AVRO)
            .setRequestedStreams(numPartitionsRequested)
            .setReadOptions(readOptions)
            .setTableReference(tableReference)
            // The BALANCED sharding strategy causes the server to assign roughly the same
            // number of rows to each stream.
            .setShardingStrategy(ShardingStrategy.BALANCED)
            .build())
      val partitions = session.getStreamsList.asScala.map(_.getName)
          .zipWithIndex.map { case (name, i) => BigQueryPartition(name, i) }
          .toArray

      log.info(s"Created read session for table '$tableName': ${session.getName}")

      // This is spammy, but it will make it clear to users the number of partitions they got and
      // why.
      if (!numPartitionsRequested.equals(partitions.length)) {
        log.warn(s"Requested $numPartitionsRequested partitions, but only received " +
          s"${partitions.length} from the BigQuery Storage API for session ${session.getName}.")
      }

      BigQueryRDD.scanTable(
        sqlContext,
        partitions.asInstanceOf[Array[Partition]],
        session.getName,
        session.getAvroSchema.getSchema,
        prunedSchema,
        requiredColumns,
        options,
        getClient).asInstanceOf[RDD[Row]]

    } finally {
      // scanTable returns immediately not after the actual data is read.
      client.close()
    }
  }

  /**
   * The theoretical number of Partitions of the returned DataFrame.
   * If the table is small the server will provide fewer readers and there will be fewer
   * partitions.
   *
   * VisibleForTesting
   */
  def getNumPartitionsRequested: Int = options.parallelism
      .getOrElse(Math.max((sizeInBytes / DEFAULT_BYTES_PER_PARTITION).toInt, 1))

  // VisibleForTesting
  private[bigquery] def getCompiledFilter(filters: Array[Filter]): String = {
    if(options.combinePushedDownFilters) {
      // new behaviour, fixing https://github.com/GoogleCloudPlatform/spark-bigquery-connector/issues/74
      Seq(
        options.filter,
        BigQueryUtil.noneIfEmpty(DirectBigQueryRelation.compileFilters(handledFilters(filters)))
      )
        .flatten
        .map(f => s"($f)")
        .mkString(" AND ")
    } else {
      // old behaviour, kept for backward compatibility
      // If a manual filter has been specified do not push down anything.
      options.filter.getOrElse {
        // TODO(pclay): Figure out why there are unhandled filters after we already listed them
        DirectBigQueryRelation.compileFilters(handledFilters(filters))
      }
    }
  }

  private def handledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(filter => DirectBigQueryRelation.isHandled(filter))
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    // If a manual filter has been specified tell Spark they are all unhandled
    if (options.filter.isDefined) {
      return filters
    }

    val unhandled = filters.filterNot(handledFilters(filters).contains)
    log.debug(s"unhandledFilters: ${unhandled.mkString(" ")}")
    unhandled
  }
}

object DirectBigQueryRelation {
  def createReadClient(options: SparkBigQueryOptions): BigQueryStorageClient = {
    // TODO(pmkc): investigate thread pool sizing and log spam matching
    // https://github.com/grpc/grpc-java/issues/4544 in integration tests
    var clientSettings = BigQueryStorageSettings.newBuilder()
        .setTransportChannelProvider(
          BigQueryStorageSettings.defaultGrpcTransportProviderBuilder()
              .setHeaderProvider(
                FixedHeaderProvider.create("user-agent", BuildInfo.name + "/" + BuildInfo.version))
              .build())
    options.createCredentials match {
      case Some(creds) => clientSettings.setCredentialsProvider(
        new CredentialsProvider {
          override def getCredentials: Credentials = creds
        })
      case None =>
    }

    BigQueryStorageClient.create(clientSettings.build)
  }

  def isHandled(filter: Filter): Boolean = filter match {
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
    case And(lhs, rhs) => isHandled(lhs) && isHandled(rhs)
    case Or(lhs, rhs) => isHandled(lhs) && isHandled(rhs)
    case Not(child) => isHandled(child)
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
    case And(lhs, rhs) => Seq(lhs, rhs).map(compileFilter).map(p => s"($p)").mkString(" AND ")
    case Or(lhs, rhs) => Seq(lhs, rhs).map(compileFilter).map(p => s"($p)").mkString(" OR ")
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
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString("[", ", ", "]")
    case _ => value
  }

  private def quote(attr: String): String = {
    s"""`$attr`"""
  }
}
