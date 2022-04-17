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

import com.google.cloud.bigquery.connector.common.BigQueryPushdownException
import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.{blockStatement, makeStatement, renameColumns}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Expression, NamedExpression}

/**
 * Building blocks of a translated query, with nested subqueries.
 * Also, maintains the children, output, and projection expressions required to
 * construct the query from the bottom up
 *
 * @param expressionConverter Converter for converting Spark expressions to SQL
 * @param alias The alias for a subquery.
 * @param children A sequence containing the child queries. May be empty in the case
 *                 of a source (bottom-level) query, contain one element (for most
 *                 unary operations), or contain two elements (for joins, etc.).
 * @param projections Contains optional projection columns for this query. Currently, set in ProjectQuery and AggregateQuery
 * @param outputAttributes Optional manual override for output. Currently, set in SourceQuery and will be later used in UnionQuery
 * @param conjunctionStatement Conjunction phrase to be used in between subquery children,
 *                             or simple phrase when there are no subqueries.
 */
abstract class BQSQLQuery(
   expressionConverter: SparkExpressionConverter,
   alias: String,
   children: Seq[BQSQLQuery] = Seq.empty,
   projections: Option[Seq[NamedExpression]] = None,
   outputAttributes: Option[Seq[Attribute]] = None,
   conjunctionStatement: BigQuerySQLStatement = EmptyBigQuerySQLStatement()) {

  /**
   * Creates the sql after the FROM clause by building the queries from its children.
   * Thus, we build the sourceStatement from the bottom up
   */
  val sourceStatement: BigQuerySQLStatement =
    if (children.nonEmpty) {
      makeStatement(children.map(_.getStatement(true)), conjunctionStatement)
    } else {
      conjunctionStatement
    }

  /** Creates the last part of the sql query. This could be the WHERE clause in
   * case of a filter query or a LIMIT clause for a limit query
   */
  val suffixStatement: BigQuerySQLStatement = EmptyBigQuerySQLStatement()

  /** Gets columns from the child query */
  val columnSet: Seq[Attribute] =
    children.foldLeft(Seq.empty[Attribute])(
      (x, y) => {
        val attrs = y.outputWithQualifier
        x ++ attrs
      }
    )

  /**
   * Checks if we have already seen the AttributeReference before based on the exprId and
   * then renames the projection based on the query alias
   */
  val processedProjections: Option[Seq[NamedExpression]] = projections
    .map(
      p =>
        p.map(
          e =>
            columnSet.find(c => c.exprId == e.exprId) match {
              case Some(a) if e.isInstanceOf[AttributeReference] =>
                AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
                  a.exprId
                )
              case _ => e
            }
        )
    )
    .map(p => renameColumns(p, alias))

  /** Convert processedProjections into a BigQuerySQLStatement */
  val columns: Option[BigQuerySQLStatement] =
    processedProjections.map(
      p => makeStatement(p.map(expressionConverter.convertStatement(_, columnSet)), ",")
    )

  /**
   * Creates the output of the current query from the passed in outputAttributes
   * or projections. If both are empty, then we retrieve it from the child query's output
   */
  val output: Seq[Attribute] = {
    outputAttributes.getOrElse(
      processedProjections.map(p => p.map(_.toAttribute)).getOrElse {
        if (children.isEmpty) {
          throw new BigQueryPushdownException(
            "Query output attributes must not be empty when it has no children."
          )
        } else {
          children
            .foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.output)
        }
      }
    )
  }

  /** Add the query alias as a qualifier for each output attribute */
  val outputWithQualifier: Seq[AttributeReference] = output.map(
    a =>
      AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
        a.exprId,
        Seq[String](alias)
      )
  )

  /** Converts this query into a String representing the SQL.
   *
   * @param useAlias Whether or not to alias this translated block of SQL.
   * @return SQL statement for this query.
   */
  def getStatement(useAlias: Boolean = false): BigQuerySQLStatement = {
    val statement =
      ConstantString("SELECT") + columns.getOrElse(ConstantString("*").toStatement) + "FROM" +
        sourceStatement + suffixStatement

    if (useAlias) {
      blockStatement(statement, alias)
    } else {
      statement
    }
  }

  /** Fetch the output attributes for Spark. */
  def getOutput: Seq[Attribute] = {
    output.map { col => Alias(Cast(col, col.dataType), col.name)(
      col.exprId,
      Seq.empty[String],
      Some(col.metadata)
    ) }.map(_.toAttribute)
  }

  /**
   * Convert Spark expression to BigQuerySQLStatement by using the passed in expressionConverter
   * @param expr
   * @return
   */
  def expressionToStatement(expr: Expression): BigQuerySQLStatement =
    expressionConverter.convertStatement(expr, columnSet)
}

/** The base query representing a BigQuery table
 *
 * @constructor
 * @param tableName   The BigQuery table to be queried
 * @param outputAttributes  Columns used to override the output generation
 *                    These are the columns resolved by DirectBigQueryRelation.
 * @param alias      Query alias.
 */
case class SourceQuery(
  expressionConverter: SparkExpressionConverter,
  tableName: String,
  outputAttributes: Seq[Attribute],
  alias: String)
  extends BQSQLQuery(
    expressionConverter,
    alias,
    outputAttributes = Some(outputAttributes),
    conjunctionStatement = ConstantString("`" + tableName + "`").toStatement + ConstantString("AS BQ_CONNECTOR_QUERY_ALIAS")) {}

/** Query for filter operations.
 *
 * @constructor
 * @param conditions The filter condition.
 * @param child      The child node.
 * @param alias      Query alias.
 */
case class FilterQuery(
  expressionConverter: SparkExpressionConverter,
  conditions: Seq[Expression],
  child: BQSQLQuery,
  alias: String)
  extends BQSQLQuery(
    expressionConverter,
    alias,
    children = Seq(child)) {

  /** Builds the WHERE statement of the filter query */
  override val suffixStatement: BigQuerySQLStatement =
    ConstantString("WHERE") + makeStatement(
      conditions.map(expressionToStatement),
      "AND"
    )
}

/** Query for projection operations.
 *
 * @constructor
 * @param projectionColumns The projection columns.
 * @param child   The child node.
 * @param alias   Query alias.
 */
case class ProjectQuery(
  expressionConverter: SparkExpressionConverter,
  projectionColumns: Seq[NamedExpression],
  child: BQSQLQuery,
  alias: String)
  extends BQSQLQuery(
    expressionConverter,
    alias, children = Seq(child),
    projections = Some(projectionColumns)) {}

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
  child: BQSQLQuery,
  alias: String
 ) extends BQSQLQuery(
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

/** Query for Sort and Limit operations.
 *
 * @constructor
 * @param limit   Limit expression.
 * @param orderBy Order By expressions.
 * @param child   The child node.
 * @param alias   Query alias.
 */
case class SortLimitQuery(
  expressionConverter: SparkExpressionConverter,
  limit: Option[Expression],
  orderBy: Seq[Expression],
  child: BQSQLQuery,
  alias: String)
  extends BQSQLQuery(
    expressionConverter,
    alias,
    children = Seq(child)) {

  /** Builds the ORDER BY clause of the sort query and/or LIMIT clause of the sort query */
  override val suffixStatement: BigQuerySQLStatement = {
    val statementFirstPart =
      if (orderBy.nonEmpty) {
        ConstantString("ORDER BY") + makeStatement(
          orderBy.map(expressionToStatement),
          ","
        )
      } else {
        EmptyBigQuerySQLStatement()
      }

    statementFirstPart + limit
      .map(ConstantString("LIMIT") + expressionToStatement(_))
      .getOrElse(EmptyBigQuerySQLStatement())
  }
}
