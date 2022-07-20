package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.NamedExpression

case class WindowQuery(
                        expressionConverter: SparkExpressionConverter,
                        expressionFactory: SparkExpressionFactory,
                        windowExpressions: Seq[NamedExpression],
                        child: BigQuerySQLQuery,
                        alias: String)
  extends BigQuerySQLQuery(
      expressionConverter,
      expressionFactory,
      alias,
      children = Seq(child),
      // Need to send in the query attributes along with window expressions to avoid "TreeNodeException: Binding attribute, tree"
      projections = Some(child.outputWithQualifier ++ windowExpressions)) {}
