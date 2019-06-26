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
import com.google.cloud.bigquery.storage.v1beta1.{BigQueryStorageClient, BigQueryStorageSettings}
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions
import com.google.cloud.bigquery.storage.v1beta1.Storage.{CreateReadSessionRequest, DataFormat}
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto.TableReference
import com.google.cloud.bigquery.{Schema, StandardTableDefinition, TableDefinition, TableInfo}
import com.google.cloud.spark.bigquery.{BigQueryRelation, BuildInfo, SparkBigQueryOptions}
import com.google.protobuf.UnknownFieldSet
import com.typesafe.scalalogging.Logger
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

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

  override val needConversion: Boolean = false
  override val sizeInBytes: Long = tableDefinition.getNumBytes

  override def buildScan(): RDD[Row] = {
    buildScan(schema.fieldNames)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    buildScan(requiredColumns, Array())
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
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

    try {
      val session = client.createReadSession(
        CreateReadSessionRequest.newBuilder()
            .setParent(s"projects/${options.parentProject}")
            .setFormat(DataFormat.AVRO)
            .setRequestedStreams(getNumPartitions)
            .setReadOptions(readOptions)
            .setTableReference(tableReference)
            // TODO(aryann): Once we rebuild the generated client code, we should change
            // setUnknownFields() to use setShardingStrategy(ShardingStrategy.BALANCED). The
            // BALANCED strategy is public at the moment, so setting it using the unknown fields
            // API is safe.
            .setUnknownFields(UnknownFieldSet.newBuilder().addField(
          7, UnknownFieldSet.Field.newBuilder().addVarint(2).build()).build())
            .build())
      val partitions = session.getStreamsList.asScala.map(_.getName)
          .zipWithIndex.map { case (name, i) => BigQueryPartition(name, i) }
          .toArray

      log.info(s"Created read session for table '$tableName': ${session.getName}")
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
   * partitions. If all partitions are not read concurrently, some Partitions will be empty.
   */
  protected def getNumPartitions: Int = options.parallelism
      .getOrElse(sqlContext.sparkContext.defaultParallelism)

  private def getCompiledFilter(filters: Array[Filter]): String = {
    // If a manual filter has been specified do not push down anything.
    options.filter.getOrElse {
      // TODO(pclay): Figure out why there are unhandled filters after we already listed them
      DirectBigQueryRelation.compileFilters(handledFilters(filters))
    }
  }

  private def handledFilters(filters: Array[Filter]): Array[Filter] = {
    // We can currently only support one filter. So find first that is handled.
    filters.find(filter => DirectBigQueryRelation.isHandled(filter, schema)).toArray
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

  def isComparable(dataType: DataType): Boolean = dataType match {
    case LongType | IntegerType | ByteType => true
    case DoubleType | FloatType | ShortType => true
    case StringType => true
    case _ => false
  }

  def isHandled(filter: Filter, schema: StructType): Boolean = filter match {
    case GreaterThan(_, _) | GreaterThanOrEqual(_, _) | LessThan(_, _) | LessThanOrEqual(_, _)
    => filter.references.forall(col => isComparable(schema(col).dataType))
    case EqualTo(_, _) => true
    // Includes And, Or IsNull, is
    case _ => false
  }

  // Mostly stolen from JDBCRDD.scala

  def compileFilter(filter: Filter): String = filter match {
    case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
    case GreaterThanOrEqual(attr, value) => s"$attr >= ${compileValue(value)}"
    case LessThan(attr, value) => s"$attr < ${compileValue(value)}"
    case LessThanOrEqual(attr, value) => s"$attr <= ${compileValue(value)}"
    case EqualTo(attr, value) => s"$attr = ${compileValue(value)}"
    case _ => throw new IllegalArgumentException(s"Invalid filter: $filter")
  }

  def compileFilters(filters: Iterable[Filter]): String = {
    // TODO remove when AND is supported
    if (filters.size > 1) {
      throw new IllegalArgumentException("Cannot support multiple filters")
    }
    filters.map(compileFilter).toSeq.sorted.mkString(" AND ")
  }

  /**
   * Converts value to SQL expression.
   */
  private def compileValue(value: Any): Any = value match {
    case null => "null"
    case stringValue: String => s"'${stringValue.replace("'", "''")}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case arrayValue: Array[Any] =>
      throw new IllegalArgumentException("Cannot compare array values in filter")
    case _ => value
  }
}
