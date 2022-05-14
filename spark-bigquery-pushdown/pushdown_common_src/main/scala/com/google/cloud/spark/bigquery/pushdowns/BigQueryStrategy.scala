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

import com.google.cloud.bigquery.connector.common.{BigQueryPushdownException, BigQueryPushdownUnsupportedException}
import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Our hook into Spark that converts the logical plan into physical plan.
 * We try to translate the Spark logical plan into SQL runnable on BigQuery.
 * If the query generation fails for any reason, we return Nil and let Spark
 * run other strategies to compute the physical plan.
 *
 * Passing SparkPlanFactory as the constructor parameter here for ease of unit testing
 *
 */
class BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory) extends Strategy with Logging {

  /** This iterator automatically increments every time it is used,
   * and is for aliasing subqueries.
   */
  private final val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")

  /** Attempts to generate a SparkPlan from the provided LogicalPlan.
   *
   * @param plan The LogicalPlan provided by Spark.
   * @return An Option of Seq[BigQueryPlan] that contains the PhysicalPlan if
   *         query generation was successful, None if not.
   */
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    try {
      generateSparkPlanFromLogicalPlan(plan)
    } catch {
      case ue: BigQueryPushdownUnsupportedException =>
        logWarning(s"BigQuery doesn't support this feature :${ue.getMessage}")
        throw ue
      case e: Exception =>
        logInfo("Query pushdown failed: ", e)
        Nil
    }
  }

  def generateSparkPlanFromLogicalPlan(plan: LogicalPlan): Seq[SparkPlan] = {
    val queryRoot = generateQueryFromPlan(plan)
    val bigQueryRDDFactory = getRDDFactory(queryRoot.get)

    val sparkPlan = sparkPlanFactory.createSparkPlan(queryRoot.get, bigQueryRDDFactory.get)
    Seq(sparkPlan.get)
  }

  def getRDDFactory(queryRoot: BigQuerySQLQuery): Option[BigQueryRDDFactory] = {
    val sourceQuery = queryRoot
      .find {
        case q: SourceQuery => q
      }
      // this should ideally never happen
      .getOrElse(
        throw new BigQueryPushdownException(
          "Something went wrong: a query tree was generated with no SourceQuery."
        )
      )

    Some(sourceQuery.bigQueryRDDFactory)
  }

  /** Attempts to generate the query from the LogicalPlan by pattern matching recursively.
   * The queries are constructed from the bottom up, but the validation of
   * supported nodes for translation happens on the way down.
   *
   * @param plan The LogicalPlan to be processed.
   * @return An object of type Option[BQSQLQuery], which is None if the plan contains an
   *         unsupported node type.
   */
  def generateQueryFromPlan(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    plan match {
      case l@LogicalRelation(bqRelation: DirectBigQueryRelation, _, _, _) =>
        Some(SourceQuery(expressionConverter, expressionFactory, bqRelation.getBigQueryRDDFactory, bqRelation.getTableName, l.output, alias.next))

      case UnaryOperationExtractor(child) =>
        generateQueryFromPlan(child) map { subQuery =>
          plan match {
            case Filter(condition, _) =>
              FilterQuery(expressionConverter, expressionFactory, Seq(condition), subQuery, alias.next)

            case Project(fields, _) =>
              ProjectQuery(expressionConverter, expressionFactory, fields, subQuery, alias.next)

            case Aggregate(groups, fields, _) =>
              AggregateQuery(expressionConverter, expressionFactory, fields, groups, subQuery, alias.next)

            case Limit(limitExpr, Sort(orderExpr, true, _)) =>
              SortLimitQuery(expressionConverter, expressionFactory, Some(limitExpr), orderExpr, subQuery, alias.next)

            case Limit(limitExpr, _) =>
              SortLimitQuery(expressionConverter, expressionFactory, Some(limitExpr), Seq.empty, subQuery, alias.next)

            case Sort(orderExpr, true, Limit(limitExpr, _)) =>
              SortLimitQuery(expressionConverter, expressionFactory, Some(limitExpr), orderExpr, subQuery, alias.next)

            case Sort(orderExpr, true, _) =>
              SortLimitQuery(expressionConverter, expressionFactory, None, orderExpr, subQuery, alias.next)

            case _ => subQuery
          }
        }

      case _ =>
        throw new BigQueryPushdownUnsupportedException(
          s"Query pushdown failed in generateQueries for node ${plan.nodeName} in ${plan.getClass.getName}"
        )
    }
  }
}
