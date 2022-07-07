package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Union}

object UnionOperationExtractor {
  def unapply(node: Union): Option[Seq[LogicalPlan]] =
    Option(node match {
      case _: Union => node.children
      case _ => null
    })
}
