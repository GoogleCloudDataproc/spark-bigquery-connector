package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, Expression}
import org.apache.spark.sql.types.Metadata

class Spark33ExpressionFactory extends SparkExpressionFactory {
  override def createAlias(child: Expression, name: String, exprId: ExprId, qualifier: Seq[String], explicitMetadata: Option[Metadata]): Alias = {
    Alias(child, name)(exprId, qualifier, explicitMetadata)
  }
}
