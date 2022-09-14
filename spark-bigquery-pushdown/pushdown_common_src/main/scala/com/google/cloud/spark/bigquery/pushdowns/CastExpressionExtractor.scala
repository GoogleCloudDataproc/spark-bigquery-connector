package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}

// The Spark APIs for Cast are not compatible between Spark 2.4/3.1 and Spark 3.2/3.3.
// So, we create this extractor that can be used across all Spark versions
object CastExpressionExtractor {
  def unapply(node: Expression): Option[Expression] =
    node match {
      case _: Cast =>
        Some(node)

      case _ => None
    }
}
