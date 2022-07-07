package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.{blockStatement, makeStatement}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.Seq

case class UnionQuery(
                  expressionConverter: SparkExpressionConverter,
                  expressionFactory: SparkExpressionFactory,
                  sparkPlanFactory: SparkPlanFactory,
                  children: Seq[BigQuerySQLQuery],
                  outputAttributes: Option[Seq[Attribute]],
                  alias: String)
  extends BigQuerySQLQuery(
    expressionConverter,
    expressionFactory,
    alias,
    children = children,
    outputAttributes = outputAttributes)  {
  
  override val columnSet: Seq[Attribute] = {
    val visibleAttribute: Option[Seq[Attribute]] =
      Some(children.foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.output).map(
        a =>
          AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
            a.exprId,
            Seq[String](alias)
          )))

    children.foldLeft(Seq.empty[Attribute])(
      (x, y) => {
        val attrs = if (visibleAttribute.isEmpty) {
          y.outputWithQualifier
        } else {
          visibleAttribute.get
        }
        x ++ attrs
      }
    )
  }

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
    query
      .lift(this)
      .orElse(
        children
          .map(q => q.find(query))
          .view
          .foldLeft[Option[T]](None)(_ orElse _)
      )
}