package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.Join

// The Spark APIs for Joins are not compatible between Spark 2.4 and 3.1.
// So, we create this extractor that only extracts those parameters that are
// used for creating the join query
object JoinExtractor {
  def unapply(node: Join): Option[(JoinType, Option[Expression])] =
    node match {
      case _: Join =>
        Some(node.joinType, node.condition)

      // We should never reach here
      case _ => None
    }
}
