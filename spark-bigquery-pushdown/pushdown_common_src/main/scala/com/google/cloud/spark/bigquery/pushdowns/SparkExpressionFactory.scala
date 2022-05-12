package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, Expression}
import org.apache.spark.sql.types.Metadata

/**
 * Factory for creating Spark expressions that are not binary compatible between
 * different Spark versions.
 *
 * For example: Alias is not compatible between Spark 2.4 and Spark 3.1
 */
trait SparkExpressionFactory {
  def createAlias(child: Expression, name: String, exprId: ExprId, qualifier: Seq[String], explicitMetadata: Option[Metadata]): Alias
}
