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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Our hook into Spark that converts the logical plan into physical plan.
 * We try to translate the Spark logical plan into SQL runnable on BigQuery.
 * If the query generation fails for any reason, we return Nil and let Spark
 * run other strategies to compute the physical plan.
 *
 * Passing SparkPlanFactory as the constructor parameter here for ease of unit testing
 *
 */
abstract class BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory) extends Strategy with Logging {

  /** This iterator automatically increments every time it is used,
   * and is for aliasing subqueries.
   */
  final val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")

  /** Attempts to generate a SparkPlan from the provided LogicalPlan.
   *
   * @param plan The LogicalPlan provided by Spark.
   * @return An Option of Seq[BigQueryPlan] that contains the PhysicalPlan if
   *         query generation was successful, None if not.
   */
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    // Check if we have any unsupported nodes in the plan. If we do, we return
    // Nil and let Spark try other strategies
    if(hasUnsupportedNodes(plan)) {
      return Nil
    }

    try {
      generateSparkPlanFromLogicalPlan(plan)
    } catch {
      // We catch all exceptions here (including BigQueryPushdownUnsupportedException)
      // and return Nil because if we are not able to translate the plan, then
      // we let Spark handle it
      case e: Exception =>
        logInfo("Query pushdown failed: ", e)
        Nil
    }
  }

  def hasUnsupportedNodes(plan: LogicalPlan): Boolean = {
    plan.foreach {
      // DataSourceV2Relation is the Spark 2.4 DSv2 connector relation
      case UnaryOperationExtractor(_) | BinaryOperationExtractor(_, _) | LogicalRelation(_, _, _, _) | DataSourceV2Relation(_, _, _, _, _) | UnionOperationExtractor(_) | Expand(_, _, _) =>
      case subPlan =>
        logInfo(s"LogicalPlan has unsupported node for query pushdown : ${subPlan.nodeName} in ${subPlan.getClass.getName}")
        return true
    }

    false
  }

  def generateSparkPlanFromLogicalPlan(plan: LogicalPlan): Seq[SparkPlan] = {
    val queryRoot = generateQueryFromOriginalLogicalPlan(plan)
    val bigQueryRDDFactory = getRDDFactory(queryRoot.get)

    val sparkPlan = sparkPlanFactory.createSparkPlan(queryRoot.get, bigQueryRDDFactory.get)
    Seq(sparkPlan.get)
  }

  /**
   * Clean up the Spark generated logical plan and generate a BigQuerySQLQuery
   * from the cleaned plan
   *
   * @param plan The LogicalPlan to be processed
   * @return An object of type Option[BigQuerySQLQuery], which is None if the plan contains an
   *         unsupported node type.
   */
  def generateQueryFromOriginalLogicalPlan(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    val cleanedPlan = cleanUpLogicalPlan(plan)
    generateQueryFromPlan(cleanedPlan)
  }

  /**
   * Iterates over the logical plan to find project plan with no fields or SubQueryAlias, if found removes it
   * and returns the LogicalPlan without empty project plan, to continue the generation of query.
   * Spark is generating an empty project plan when the query has count(*)
   * Found this issue when executing query-9 of TPC-DS
   *
   * @param plan The LogicalPlan to be processed.
   * @return The processed LogicalPlan removing all the empty Project plan's or SubQueryAlias, if the input has any
   */
  def cleanUpLogicalPlan(plan: LogicalPlan): LogicalPlan = {
    plan.transform({
      case Project(Nil, child) => child
      case SubqueryAlias(_, child) => child
    })
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

  // This method will be overridden in subclasses that support query pushdown for DSv2
  def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery]

  /**
   * Attempts to generate the query from the LogicalPlan by pattern matching recursively.
   * The queries are constructed from the bottom up, but the validation of
   * supported nodes for translation happens on the way down.
   *
   * @param plan The LogicalPlan to be processed.
   * @return An object of type Option[BigQuerySQLQuery], which is None if the plan contains an
   *         unsupported node type.
   */
  def generateQueryFromPlan(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    plan match {
      case _: DataSourceV2Relation =>
        generateQueryFromPlanForDataSourceV2(plan)

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

            case Window(windowExpressions, _, _, _) =>
              WindowQuery(expressionConverter, expressionFactory, windowExpressions, subQuery,  if (plan.output.isEmpty) None else Some(plan.output), alias.next)

            case _ => subQuery
          }
        }

      case BinaryOperationExtractor(left, right) =>
        generateQueryFromPlan(left).flatMap { l =>
          generateQueryFromPlan(right) map { r =>
            plan match {
              case JoinExtractor(joinType, condition) =>
                joinType match {
                  case Inner | LeftOuter | RightOuter | FullOuter =>
                    JoinQuery(expressionConverter, expressionFactory, l, r, condition, joinType, alias.next)
                  case LeftSemi =>
                    LeftSemiJoinQuery(expressionConverter, expressionFactory, l, r, condition, isAntiJoin = false, alias)
                  case LeftAnti =>
                    LeftSemiJoinQuery(expressionConverter, expressionFactory, l, r, condition, isAntiJoin = true, alias)
                  case _ => throw new MatchError
                }
            }
          }
        }

      case UnionOperationExtractor(logicalPlanSeq) =>
        Some(UnionQuery(expressionConverter, expressionFactory, generateBigQuerySQLQueryFromLogicalPlanSeq(logicalPlanSeq), alias.next()))

      case Expand(projections, output, child) =>
        // convert list of Expressions into ProjectPlan
        val projectPlan = projections.map { p =>
          val namedExpressions = convertExpressionToNamedExpression(p, output)
          Project(namedExpressions, child)
        }
        Some(UnionQuery(expressionConverter, expressionFactory, generateBigQuerySQLQueryFromLogicalPlanSeq(projectPlan), alias.next()))

      case _ =>
        throw new BigQueryPushdownUnsupportedException(
          s"Query pushdown failed in generateQueries for node ${plan.nodeName} in ${plan.getClass.getName}"
        )
    }
  }

  /**
   * Generating list of BigQuerySQLQuery from the given list of LogicalPlan
   * @param logicalPlanSeq The LogicalPlan to be converted.
   * @return An object of type Option[BQSQLQuery].
   */
  def generateBigQuerySQLQueryFromLogicalPlanSeq(logicalPlanSeq: Seq[LogicalPlan]): Seq[BigQuerySQLQuery]

  /**
   * Method to convert Expression into NamedExpression.
   * If the Expression is not of type NamedExpression, we create an Alias from the expression and attribute
   */
  def convertExpressionToNamedExpression(projections: Seq[Expression],
                                         output: Seq[Attribute]): Seq[NamedExpression] = {
    projections zip output map { expression =>
      expression._1 match {
        case expr: NamedExpression => expr
        case _ => expressionFactory.createAlias(expression._1, expression._2.name, expression._2.exprId, Seq.empty[String], Some(expression._2.metadata))
      }
    }
  }

}
