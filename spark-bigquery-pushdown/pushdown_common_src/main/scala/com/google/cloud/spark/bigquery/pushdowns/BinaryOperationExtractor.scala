package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Join, LogicalPlan}

/** Extractor for supported binary operations. */
object BinaryOperationExtractor {
  def unapply(node: BinaryNode): Option[(LogicalPlan, LogicalPlan)] =
    Option(node match {
      case _: Join => (node.left, node.right)
      case _ => null
    })
}
