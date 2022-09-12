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
import com.google.cloud.spark.bigquery.SparkBigQueryUtil.isDataFrameShowMethodInStackTrace
import com.google.cloud.spark.bigquery.direct.{BigQueryRDDFactory, DirectBigQueryRelation}
import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.{convertExpressionToNamedExpression, removeProjectNodeFromPlan}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, NamedExpression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

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
      case UnaryOperationExtractor(_) | BinaryOperationExtractor(_, _)  | UnionOperationExtractor(_) =>

      // NamedRelation is the superclass of DataSourceV2Relation and DataSourceV2ScanRelation.
      // DataSourceV2Relation is the Spark 2.4 DSv2 connector relation and
      // DataSourceV2ScanRelation is the Spark 3.1 DSv2 connector relation
      case _: LogicalRelation | _: NamedRelation | _: Expand =>

      case subPlan =>
        logInfo(s"LogicalPlan has unsupported node for query pushdown : ${subPlan.nodeName} in ${subPlan.getClass.getName}")
        return true
    }

    false
  }

  def generateSparkPlanFromLogicalPlan(plan: LogicalPlan): Seq[SparkPlan] = {
    val cleanedPlan = cleanUpLogicalPlan(plan)

    // We have to handle the case where df.show() was called by an end user
    // differently because Spark can insert a Project node on top of the
    // LogicalPlan and that could mess up the ordering of results.
    if (isDataFrameShowMethodInStackTrace) {

      // We first get the project node that has been added by Spark for df.show()
      val projectNodeAddedBySpark = getTopMostProjectNodeWithAliasedCasts(cleanedPlan)

      // If the Project node exists, we first create a BigQueryPlan after
      // removing the extra Project node and then create a physical Project plan
      // by passing the BigQueryPlan as its child. Note that Spark may not add the
      // Project node if the selected(projected) columns are all of type string.
      // In such a case, we don't have to return a ProjectExec SparkPlan
      if (projectNodeAddedBySpark.isDefined) {
        return Seq(generateProjectPlanFromLogicalPlan(cleanedPlan, projectNodeAddedBySpark.get))
      }
    }

    Seq(generateBigQueryPlanFromLogicalPlan(cleanedPlan))
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

  /**
   * Returns the first Project node that contains expressions in the projectList
   * of the form Alias(Cast(_,_,_)) while recursing down the LogicalPlan.
   * If an Aggregate, Join or Relation node is encountered first, we return None
   */
  def getTopMostProjectNodeWithAliasedCasts(plan: LogicalPlan): Option[Project] = {
    plan.foreach {
      case _: Aggregate | _: Join | _: LogicalRelation | _:NamedRelation =>
        return None

      case projectNode@Project(projectList, _) =>
        projectList.foreach {
          case Alias(child, _) =>
            child match {
              case CastExpressionExtractor(_) => return Some(projectNode)

              case _ =>
            }
          case _ =>
        }
        return None

      case _ =>
    }

    // We should never reach here
    None
  }

  /**
   * Generates the BigQueryPlan from the Logical plan by
   * 1) Generating a BigQuerySQLQuery from the passed-in LogicalPlan
   * 2) Getting the BigQueryRDDFactory from the source query
   * 3) Creating a BigQueryPlan by passing the query root and the BigQueryRDDFactory
   */
  def generateBigQueryPlanFromLogicalPlan(plan: LogicalPlan): SparkPlan = {
    // Generate the query from the logical plan
    val queryRoot = generateQueryFromPlan(plan)
    val bigQueryRDDFactory = getRDDFactory(queryRoot.get)

    // Create the SparkPlan
    sparkPlanFactory.createBigQueryPlan(queryRoot.get, bigQueryRDDFactory.get)
      .getOrElse(throw new BigQueryPushdownException("Could not generate BigQuery physical plan from query"))
  }

  /**
   * Generates the physical Project plan from the Logical plan by
   * 1) Removing the passed-in Project node from the passed-in plan
   * 2) Generating a BigQuery plan from the remaining plan
   * 3) Creating a physical Project plan by passing the Project node and BigQueryPlan into the SparkPlanFactory
   */
  def generateProjectPlanFromLogicalPlan(plan: LogicalPlan, projectNode: Project): SparkPlan = {
    // Remove Project node from the plan
    val planWithoutProjectNode = removeProjectNodeFromPlan(plan, projectNode)
    val bigQueryPlan = generateBigQueryPlanFromLogicalPlan(planWithoutProjectNode)
    sparkPlanFactory.createProjectPlan(projectNode, bigQueryPlan)
      .getOrElse(throw new BigQueryPushdownException("Could not generate BigQuery physical plan from query"))
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
      // NamedRelation is the superclass of DataSourceV2Relation and DataSourceV2ScanRelation.
      // DataSourceV2Relation is the Spark 2.4 DSv2 connector relation and
      // DataSourceV2ScanRelation is the Spark 3.1 DSv2 connector relation
      case _: NamedRelation =>
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

            // Special case for Spark 2.4 in which Spark SQL query already has a limit clause and df.show() is called
            case Limit(limitExpr, Limit(_, Sort(orderExpr, true, _))) =>
              SortLimitQuery(expressionConverter, expressionFactory, Some(limitExpr), orderExpr, subQuery, alias.next)

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
                  case Inner | LeftOuter | RightOuter | FullOuter | Cross =>
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

      case UnionOperationExtractor(children) =>
        createUnionQuery(children)

      case Expand(projections, output, child) =>
        // convert list of Expressions into ProjectPlan
        val projectPlan = projections.map { p =>
          val namedExpressions = convertExpressionToNamedExpression(p, output, expressionFactory)
          Project(namedExpressions, child)
        }
        createUnionQuery(projectPlan)

      case _ =>
        throw new BigQueryPushdownUnsupportedException(
          s"Query pushdown failed in generateQueries for node ${plan.nodeName} in ${plan.getClass.getName}"
        )
    }
  }

  /**
   * Create a Union BigQuerySQLQuery from the given list of LogicalPlan
   * @param children The list of LogicalPlan to be converted.
   * @return An object of type Option[BigQuerySQLQuery].
   */
  def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery]
}
