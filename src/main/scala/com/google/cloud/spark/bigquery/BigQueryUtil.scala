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

import com.google.cloud.bigquery.{BigQueryError, BigQueryException, TableId}
import com.google.cloud.http.BaseHttpServiceException.UNKNOWN_CODE

import scala.util.matching.Regex

/**
 * Static helpers for working with BigQuery.
 */
object BigQueryUtil {

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
      project: Option[String] = None): TableId = {
    val (parsedProject, parsedDataset, table) = rawTable match {
      case QUALIFIED_TABLE_REGEX(_, _, p, d, t) => (Option(p), Option(d), t)
      case _ => throw new IllegalArgumentException(
        s"Invalid Table ID '$rawTable'. Must match '$QUALIFIED_TABLE_REGEX'")
    }
    val actualDataset = parsedDataset.orElse(dataset).getOrElse(
      throw new IllegalArgumentException("'dataset' not parsed or provided.")
    )
    parsedProject.orElse(project)
        .map(p => TableId.of(p, actualDataset, table))
        .getOrElse(TableId.of(actualDataset, table))
  }

  def noneIfEmpty(s: String): Option[String] =  Option(s).filterNot(_.trim.isEmpty)

  def convertAndThrow(error: BigQueryError) : Unit =
    throw new BigQueryException(UNKNOWN_CODE, error.getMessage, error)
}
