/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.BigQueryRDDFactory
import org.apache.spark.sql.catalyst.expressions.Attribute

/** The base query representing a BigQuery table
 *
 * @constructor
 * @param tableName   The BigQuery table to be queried
 * @param outputAttributes  Columns used to override the output generation
 *                    These are the columns resolved by DirectBigQueryRelation.
 * @param alias      Query alias.
 */
case class SourceQuery(
    expressionConverter: SparkExpressionConverter,
    bigQueryRDDFactory: BigQueryRDDFactory,
    tableName: String,
    outputAttributes: Seq[Attribute],
    alias: String)
  extends BigQuerySQLQuery(
    expressionConverter,
    alias,
    outputAttributes = Some(outputAttributes),
    conjunctionStatement = ConstantString("`" + tableName + "`").toStatement + ConstantString("AS BQ_CONNECTOR_QUERY_ALIAS")) {

  override def find[T](query: PartialFunction[BigQuerySQLQuery, T]): Option[T] = query.lift(this)
}
