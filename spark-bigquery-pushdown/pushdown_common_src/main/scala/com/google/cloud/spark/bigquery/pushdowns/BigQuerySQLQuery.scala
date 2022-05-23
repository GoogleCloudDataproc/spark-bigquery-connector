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
 * @param fields Contains output from the left + right query for left semi and left anti joins
 */
abstract class BigQuerySQLQuery(
  expressionConverter: SparkExpressionConverter,
  expressionFactory: SparkExpressionFactory,
  alias: String,
  children: Seq[BigQuerySQLQuery] = Seq.empty,
  projections: Option[Seq[NamedExpression]] = None,
  outputAttributes: Option[Seq[Attribute]] = None,
  conjunctionStatement: BigQuerySQLStatement = EmptyBigQuerySQLStatement(),
  fields: Option[Seq[Attribute]] = None) {

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

  /** Gets columns from the fields list if not empty or from the child query */
  val columnSet: Seq[Attribute] = {
    if (fields.isEmpty) {
      children.foldLeft(Seq.empty[Attribute])(
        (x, y) => {
          val attrs = y.outputWithQualifier
          x ++ attrs
        }
      )
    } else {
      fields.get
    }

  }

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
    .map(p => renameColumns(p, alias, expressionFactory))

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
  var outputWithQualifier: Seq[AttributeReference] = output.map(
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

  /**
   * Convert Spark expression to BigQuerySQLStatement by using the passed in expressionConverter
   * @param expr
   * @return
   */
  def expressionToStatement(expr: Expression): BigQuerySQLStatement =
    expressionConverter.convertStatement(expr, columnSet)

  /** Finds a particular query type in the overall tree.
   *
   * @param query PartialFunction defining a positive result.
   * @param T BigQuerySQLQuery type
   * @return Option[T] for one positive match, or None if nothing found.
   */
  def find[T](query: PartialFunction[BigQuerySQLQuery, T]): Option[T] =
    query
      .lift(this)
      .orElse(
        if (this.children.isEmpty) {
          None
        } else {
          this.children.head.find(query)
        }
      )

  // For OUTER JOIN, the column's nullability may need to be modified as true
  def nullableOutputWithQualifier: Seq[AttributeReference] = output.map(
    a =>
      AttributeReference(a.name, a.dataType, nullable = true, a.metadata)(
        a.exprId,
        Seq[String](alias)
      )
  )
}
