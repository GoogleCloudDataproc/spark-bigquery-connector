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

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.{blockStatement, makeStatement}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.Seq

case class UnionQuery(
                  expressionConverter: SparkExpressionConverter,
                  expressionFactory: SparkExpressionFactory,
                  children: Seq[BigQuerySQLQuery],
                  alias: String)
  extends BigQuerySQLQuery(
    expressionConverter,
    expressionFactory,
    alias,
    children = children,
    outputAttributes = if (children.isEmpty) None else Some(children.head.output),
    visibleAttributeOverride = if (children.isEmpty) None else Some(children.foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.output).map(
      a =>
        AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
          a.exprId,
          Seq[String](alias)
        )))) {

  override def getStatement(useAlias: Boolean): BigQuerySQLStatement = {
    val query =
      if (children.nonEmpty) {
        makeStatement(
          children.map(c => blockStatement(c.getStatement())),
          "UNION ALL"
        )
      } else {
        EmptyBigQuerySQLStatement()
      }

    if (useAlias) {
      blockStatement(query, alias)
    } else {
      query
    }
  }

  override def find[T](query: PartialFunction[BigQuerySQLQuery, T]): Option[T] =
    query.lift(this).orElse(
        children
          .map(q => q.find(query))
          .view
          .foldLeft[Option[T]](None)(_ orElse _)
      )
}
