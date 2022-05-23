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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

/** Query for filter operations.
 *
 * @constructor
 * @param conditions The filter condition.
 * @param child      The child node.
 * @param alias      Query alias.
 * @param fields Contains output from the left + right query for left semi and left anti joins
 */
case class FilterQuery(
    expressionConverter: SparkExpressionConverter,
    expressionFactory: SparkExpressionFactory,
    conditions: Seq[Expression],
    child: BigQuerySQLQuery,
    alias: String,
    fields: Option[Seq[Attribute]] = None)
  extends BigQuerySQLQuery(
    expressionConverter,
    expressionFactory,
    alias,
    children = Seq(child),
    fields = fields) {

  /** Builds the WHERE statement of the filter query */
  override val suffixStatement: BigQuerySQLStatement =
    ConstantString("WHERE") + makeStatement(
      conditions.map(expressionToStatement),
      "AND"
    )
}
