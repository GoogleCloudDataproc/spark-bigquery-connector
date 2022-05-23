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

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.blockStatement
import org.apache.spark.sql.catalyst.expressions.Expression

case class LeftSemiJoinQuery(
   expressionConverter: SparkExpressionConverter,
   expressionFactory: SparkExpressionFactory,
   left: BigQuerySQLQuery,
   right: BigQuerySQLQuery,
   conditions: Option[Expression],
   isAntiJoin: Boolean = false,
   alias: Iterator[String])
  extends BigQuerySQLQuery(expressionConverter, expressionFactory, alias.next, Seq(left), Some(left.outputWithQualifier), outputAttributes = None) {

  override val suffixStatement: BigQuerySQLStatement =
    ConstantString("WHERE") + (if (isAntiJoin) " NOT " else " ") + "EXISTS" + blockStatement(
      FilterQuery(
        expressionConverter,
        expressionFactory,
        conditions = if (conditions.isEmpty) Seq.empty else Seq(conditions.get),
        child = right,
        alias = alias.next,
        fields = Some(
          left.outputWithQualifier ++ right.outputWithQualifier
        )
      ).getStatement()
    )

  override def find[T](query: PartialFunction[BigQuerySQLQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}
