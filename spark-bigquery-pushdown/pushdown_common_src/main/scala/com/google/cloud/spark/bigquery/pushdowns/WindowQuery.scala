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
