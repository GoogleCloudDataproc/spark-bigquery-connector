package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, LocalLimit, LogicalPlan, Project, ReturnAnswer, Sort, UnaryNode, Window}

/** Extractor for supported unary operations. */
object UnaryOperationExtractor {

  def unapply(node: UnaryNode): Option[LogicalPlan] =
    node match {
      case _: Filter | _: Project | _: GlobalLimit | _: LocalLimit |
           _: Aggregate | _: Sort | _: ReturnAnswer | _: Window =>
        Some(node.child)

      case _ => None
    }
}
