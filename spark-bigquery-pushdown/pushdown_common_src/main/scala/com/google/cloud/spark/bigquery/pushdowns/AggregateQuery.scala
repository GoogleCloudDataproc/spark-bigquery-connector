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

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.makeStatement
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}

/** Query for aggregation operations.
 *
 * @constructor
 * @param projectionColumns The projection columns, containing also the aggregate expressions.
 * @param groups  The grouping columns.
 * @param child   The child node.
 * @param alias   Query alias.
 */
case class AggregateQuery(
   expressionConverter: SparkExpressionConverter,
   projectionColumns: Seq[NamedExpression],
   groups: Seq[Expression],
   child: BigQuerySQLQuery,
   alias: String
 ) extends BigQuerySQLQuery(
  expressionConverter,
  alias,
  children = Seq(child),
  projections = if (projectionColumns.isEmpty) None else Some(projectionColumns)) {

  /** Builds the GROUP BY clause of the filter query.
   * Insert a limit 1 to ensure that only one row returns if there are no grouping expressions
   * */
  override val suffixStatement: BigQuerySQLStatement =
    if (groups.nonEmpty) {
      ConstantString("GROUP BY") +
        makeStatement(groups.map(expressionToStatement), ",")
    } else {
      ConstantString("LIMIT 1").toStatement
    }
}
