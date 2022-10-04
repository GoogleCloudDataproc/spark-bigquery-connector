package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}

/** Extractor for supported binary operations. Returns the node itself if the
 * operation is supported **/
object BinaryOperationExtractor {
  def unapply(node: LogicalPlan): Option[LogicalPlan] =
    Option(node match {
      case _: Join => node
      case _ => null
    })
}
