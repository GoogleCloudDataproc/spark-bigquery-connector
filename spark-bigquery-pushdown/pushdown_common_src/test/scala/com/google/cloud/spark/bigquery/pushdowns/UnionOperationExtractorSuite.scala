package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.plans.logical.{Range, Union}
import org.scalatest.funsuite.AnyFunSuite

class UnionOperationExtractorSuite extends AnyFunSuite{

  // Need a sequence of logicalplan's to pass. So, create the simplest possible
  private val childPlan1 = Range.apply(2L, 100L, 4L, 8)
  private val childPlan2 = Range.apply(4L, 200L, 8L, 16)

  test(testName = "Union") {
    val union = Union(Seq(childPlan1, childPlan2))
    val plan = UnionOperationExtractor.unapply(union)
    assert(plan.isDefined)
    assert(plan.get.length == 2)
    assert(plan.get.head.isInstanceOf[Range])
    assert(plan.get.equals(Seq(childPlan1, childPlan2)))
  }

}
