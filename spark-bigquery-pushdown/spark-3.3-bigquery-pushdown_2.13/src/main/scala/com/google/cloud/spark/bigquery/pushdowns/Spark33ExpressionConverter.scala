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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, CheckOverflow, Expression, ScalarSubquery, UnaryMinus}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Convert Spark 3.3 specific expressions to SQL
 */
class Spark33ExpressionConverter(expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory) extends SparkExpressionConverter() {
  override def convertScalarSubqueryExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    expression match {
      case ScalarSubquery(plan, _, _, joinCond) if joinCond.isEmpty =>
        blockStatement(new Spark33BigQueryStrategy(this, expressionFactory, sparkPlanFactory)
          .generateQueryFromPlan(plan).get.getStatement())
    }
  }

  override def convertCheckOverflowExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    expression match {
      case CheckOverflow(child, t, _) =>
        getCastType(t) match {
          case Some(cast) =>
            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + "AS" + cast)
          case _ => convertStatement(child, fields)
        }
    }
  }

  override def convertUnaryMinusExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    expression match {
      case UnaryMinus(child, _) =>
        ConstantString("-") +
          blockStatement(convertStatement(child, fields))
    }
  }

  override def convertCastExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    expression match {
      case Cast(child, dataType, _, ansiEnabled) if !ansiEnabled =>
        performCastExpressionConversion(child, fields, dataType)
    }
  }
}
