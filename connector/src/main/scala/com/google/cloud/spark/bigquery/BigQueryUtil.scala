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

import java.util.{Optional, Properties}

import com.google.cloud.bigquery.{BigQuery, BigQueryError, BigQueryException, BigQueryOptions, TableId}
import com.google.cloud.http.BaseHttpServiceException.UNKNOWN_CODE

import scala.util.matching.Regex
import scala.collection.JavaConverters._
import io.grpc.StatusRuntimeException
import com.google.api.gax.rpc.StatusCode
import com.google.auth.Credentials
import io.grpc.Status
import org.apache.spark.internal.Logging

/**
 * Static helpers for working with BigQuery.
 */
object BigQueryUtil extends Logging{

  private val PROJECT_PATTERN = """\S+"""
  private val DATASET_PATTERN = """\w+"""

  // Allow all non-whitespace beside ':' and '.'.
  // These confuse the qualified table parsing.
  private val TABLE_PATTERN = """[\S&&[^.:]]+"""

  /**
   * Regex for an optionally fully qualified table.
   *
   * Must match 'project.dataset.table' OR the legacy 'project:dataset.table' OR 'dataset.table'
   * OR 'table'.
   */
  val QUALIFIED_TABLE_REGEX: Regex =
    s"""^((($PROJECT_PATTERN)[:.])?($DATASET_PATTERN)\\.)?($TABLE_PATTERN)$$""".r

  def friendlyTableName(tableId: TableId): String = {
    (Option(tableId.getProject), tableId.getDataset, tableId.getTable) match {
      case (Some(project), dataset, table) => s"$project.$dataset.$table"
      case (None, dataset, table) => s"$dataset.$table"
    }
  }

  def parseTableId(
      rawTable: String,
      dataset: Option[String] = None,
      project: Option[String] = None,
      datePartition: Option[String] = None): TableId = {
    val (parsedProject, parsedDataset, table) = rawTable match {
      case QUALIFIED_TABLE_REGEX(_, _, p, d, t) => (Option(p), Option(d), t)
      case _ => throw new IllegalArgumentException(
        s"Invalid Table ID '$rawTable'. Must match '$QUALIFIED_TABLE_REGEX'")
    }
    val actualDataset = parsedDataset.orElse(dataset).getOrElse(
      throw new IllegalArgumentException("'dataset' not parsed or provided.")
    )
    // adding partition if needed
    val tableAndPartition = datePartition
      .map(date => s"${table}$$${date}")
      .getOrElse(table)
    parsedProject.orElse(project)
      .map(p => TableId.of(p, actualDataset, tableAndPartition))
      .getOrElse(TableId.of(actualDataset, tableAndPartition))
  }

  def noneIfEmpty(s: String): Option[String] = Option(s).filterNot(_.trim.isEmpty)

  def convertAndThrow(error: BigQueryError) : Unit =
    throw new BigQueryException(UNKNOWN_CODE, error.getMessage, error)

  def isRetryable(cause: Throwable): Boolean = {
    var c: Throwable = cause
    while(c != null) {
      if (isRetryableInternalError(c)) {
        return true
      }
      c = c.getCause
    }
    // failed
    false
  }

  val InternalErrorMessages = Seq(
      "HTTP/2 error code: INTERNAL_ERROR",
      "Connection closed with unknown cause",
      "Received unexpected EOS on DATA frame from server")

  def isRetryableInternalError(cause: Throwable) : Boolean = {
    cause match {
      case sse: StatusRuntimeException =>
        sse.getStatus.getCode == Status.Code.INTERNAL &&
        InternalErrorMessages.exists(errorMsg => cause.getMessage.contains(errorMsg))
      case _ => false
    }
  }

  // validating that the connector's scala version and the runtime's scala
  // version are the same
  def validateScalaVersionCompatibility(): Unit = {
    val runtimeScalaVersion = trimVersion(scala.util.Properties.versionNumberString)
    val buildProperties = new Properties
    buildProperties.load(getClass.getResourceAsStream("/spark-bigquery-connector.properties"))
    val connectorScalaVersion = trimVersion(buildProperties.getProperty("scala.version"))
    if (!runtimeScalaVersion.equals(connectorScalaVersion)) {
      throw new IllegalStateException(
        s"""
           |This connector was made for Scala $connectorScalaVersion,
           |it was not meant to run on Scala $runtimeScalaVersion"""
          .stripMargin.replace('\n', ' '))
    }
  }

  private def trimVersion(version: String) =
    version.substring(0, version.lastIndexOf('.'))

  def toSeq[T](list: java.util.List[T]): Seq[T] = list.asScala.toSeq

  def toJavaIterator[T](it: Iterator[T]): java.util.Iterator[T] = it.asJava


  def createBigQuery(options: SparkBigQueryConfig): BigQuery = {
    val credentials = options.createCredentials()
    val parentProjectId = options.getParentProjectId()
    logInfo(
      s"BigQuery client project id is [$parentProjectId}], derived from teh parentProject option")
    BigQueryOptions
      .newBuilder()
      .setProjectId(parentProjectId)
      .setCredentials(credentials)
      .build()
      .getService
  }

  def toOption[T](javaOptional: Optional[T]): Option[T] =
    if (javaOptional.isPresent) Some(javaOptional.get) else None
}
