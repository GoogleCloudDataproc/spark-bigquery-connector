package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.NamedExpression

case class WindowQuery(
                        expressionConverter: SparkExpressionConverter,
                        expressionFactory: SparkExpressionFactory,
                        projections: Option[Seq[NamedExpression]],
                        child: BigQuerySQLQuery,
                        alias: String)
  extends BigQuerySQLQuery(
      expressionConverter,
      expressionFactory,
      alias,
      children = Seq(child),
      projections = projections) {}
