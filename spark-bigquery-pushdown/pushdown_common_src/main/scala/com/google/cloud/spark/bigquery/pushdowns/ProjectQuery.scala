package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.NamedExpression

/** Query for projection operations.
 *
 * @constructor
 * @param projectionColumns The projection columns.
 * @param child   The child node.
 * @param alias   Query alias.
 */
case class ProjectQuery(
     expressionConverter: SparkExpressionConverter,
     expressionFactory: SparkExpressionFactory,
     projectionColumns: Seq[NamedExpression],
     child: BigQuerySQLQuery,
     alias: String)
  extends BigQuerySQLQuery(
    expressionConverter,
    expressionFactory,
    alias, children = Seq(child),
    projections = Some(projectionColumns)) {}
