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
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Our hook into Spark that converts the logical plan into physical plan.
 * Tries to translate the Spark logical plan into SQL runnable on BigQuery.
 *
 */
class BigQueryStrategy(expressionConverter: SparkExpressionConverter) extends Strategy with Logging {

  /** This iterator automatically increments every time it is used,
   * and is for aliasing subqueries.
   */
  private final val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")

  /**
   * Relation at the base of the logical plan
   */
  private var directBigQueryRelation: Option[DirectBigQueryRelation] = None

  /** Attempts to get a SparkPlan from the provided LogicalPlan. If the query
   * generation fails for any reason, we return Nil and let Spark run other
   * strategies to compute the physical plan
   *
   * @param plan The LogicalPlan provided by Spark.
   * @return An Option of Seq[BigQueryPlan] that contains the PhysicalPlan if
   *         query generation was successful, None if not.
   */
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val cleanedPlan = cleanUpLogicalPlan(plan)

    val queryRoot: Option[BQSQLQuery] = {
      try {
        generateQueryFromPlan(cleanedPlan)
      } catch {
        case e: Exception =>
          logInfo("Query pushdown failed: ", e)
          None
      }
    }

    if (queryRoot.isEmpty) {
      return Nil
    }

    val sparkPlan = generateSparkPlan(queryRoot.get)
    if (sparkPlan.isEmpty) {
      return Nil
    }

    Seq(sparkPlan.get)
  }

  /**
   * Remove redundant nodes from the plan
   */
  def cleanUpLogicalPlan(plan: LogicalPlan): LogicalPlan = {
    plan.transform({
      case Project(Nil, child) => child
      case SubqueryAlias(_, child) => child
    })
  }

  /** Attempts to generate the query from the LogicalPlan by pattern matching recursively.
   * The queries are constructed from the bottom up, but the validation of
   * supported nodes for translation happens on the way down.
   *
   * @param plan The LogicalPlan to be processed.
   * @return An object of type Option[BQSQLQuery], which is None if the plan contains an
   *         unsupported node type.
   */
  private def generateQueryFromPlan(plan: LogicalPlan): Option[BQSQLQuery] = {
    plan match {
      case l@LogicalRelation(bqRelation: DirectBigQueryRelation, _, _, _) =>
        directBigQueryRelation = Some(bqRelation)
        Some(SourceQuery(expressionConverter, bqRelation.tableName, l.output, alias.next))

      case UnaryOperationExtractor(child) =>
        generateQueryFromPlan(child) map { subQuery =>
          plan match {
            case Filter(condition, _) =>
              FilterQuery(expressionConverter, Seq(condition), subQuery, alias.next)

            case Project(fields, _) =>
              ProjectQuery(expressionConverter, fields, subQuery, alias.next)

            case Aggregate(groups, fields, _) =>
              AggregateQuery(expressionConverter, fields, groups, subQuery, alias.next)

            case Limit(limitExpr, Sort(orderExpr, true, _)) =>
              SortLimitQuery(expressionConverter, Some(limitExpr), orderExpr, subQuery, alias.next)

            case Limit(limitExpr, _) =>
              SortLimitQuery(expressionConverter, Some(limitExpr), Seq.empty, subQuery, alias.next)

            case Sort(orderExpr, true, Limit(limitExpr, _)) =>
              SortLimitQuery(expressionConverter, Some(limitExpr), orderExpr, subQuery, alias.next)

            case Sort(orderExpr, true, _) =>
              SortLimitQuery(expressionConverter, None, orderExpr, subQuery, alias.next)

            case _ => subQuery
          }
        }

      case _ =>
        throw new BigQueryPushdownUnsupportedException(
          s"Query pushdown failed in generateQueries for node ${plan.nodeName} in ${plan.getClass.getName}"
        )
    }
  }

  /**
   * Generate SparkPlan from the output and RDD of the translated query
   */
  private def generateSparkPlan(queryRoot: BQSQLQuery): Option[SparkPlan] = {
    try {
      Some(BigQueryPlan(queryRoot.getOutput, getRdd(queryRoot)))
    } catch {
      case e: Exception =>
        logInfo("Query pushdown failed: ", e)
        None
    }
  }

  /**
   * Create RDD from the SQL statement of the translated query
   */
  private def getRdd(queryRoot: BQSQLQuery): RDD[InternalRow] = {
    if (directBigQueryRelation.isEmpty) {
      throw new BigQueryPushdownException(
        "Cannot generate RDD from the logical plan since the base relation is not set"
      )
    }

    directBigQueryRelation.get.buildScanFromSQL(queryRoot.getStatement().toString)
  }
}
