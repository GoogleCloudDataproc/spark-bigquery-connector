package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.{blockStatement, makeStatement}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.Seq

case class UnionQuery(
                  expressionConverter: SparkExpressionConverter,
                  expressionFactory: SparkExpressionFactory,
                  children: Seq[BigQuerySQLQuery],
                  outputAttributes: Option[Seq[Attribute]],
                  visibleAttribute: Option[Seq[Attribute]],
                  alias: String)
  extends BigQuerySQLQuery(
    expressionConverter,
    expressionFactory,
    alias,
    children = children,
    outputAttributes = outputAttributes,
    visibleAttribute = visibleAttribute)  {

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
