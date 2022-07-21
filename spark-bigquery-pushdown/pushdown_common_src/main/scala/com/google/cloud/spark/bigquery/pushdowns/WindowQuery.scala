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

import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}

case class WindowQuery(
                        expressionConverter: SparkExpressionConverter,
                        expressionFactory: SparkExpressionFactory,
                        windowExpressions: Seq[NamedExpression],
                        child: BigQuerySQLQuery,
                        fields: Option[Seq[Attribute]],
                        alias: String)
  extends BigQuerySQLQuery(
      expressionConverter,
      expressionFactory,
      alias,
      children = Seq(child),
      // Need to send in the query attributes along with window expressions to avoid "TreeNodeException: Binding attribute, tree"
      projections = WindowQuery.getWindowProjections(windowExpressions, child, fields)) {}

object WindowQuery {
      def getWindowProjections(windowExpressions: Seq[NamedExpression], child: BigQuerySQLQuery, fields: Option[Seq[Attribute]]): Option[Seq[NamedExpression]] = {
            val projectionVector = windowExpressions ++ child.outputWithQualifier

            val orderedProjections =
                  fields.map(_.map(reference => {
                        val origPos = projectionVector.map(_.exprId).indexOf(reference.exprId)
                        projectionVector(origPos)
                  }))

            orderedProjections
      }
}
