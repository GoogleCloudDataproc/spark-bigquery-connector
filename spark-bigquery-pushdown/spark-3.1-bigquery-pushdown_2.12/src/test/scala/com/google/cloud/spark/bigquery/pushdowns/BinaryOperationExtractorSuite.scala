package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Intersect, Join, JoinHint, Range}
import org.scalatest.funsuite.AnyFunSuite

class BinaryOperationExtractorSuite extends AnyFunSuite  {

  // Need a childPlan to pass. So, create the simplest possible
  private val leftChildPlan = Range.apply(2L, 100L, 4L, 8)
  private val rightChildPlan = Range.apply(2L, 100L, 4L, 10)

  test("supported binary node") {
    val joinPlan = Join(leftChildPlan, rightChildPlan, JoinType.apply("inner"), None, JoinHint.NONE)
    val plan = BinaryOperationExtractor.unapply(joinPlan)
    assert(plan.isDefined)
    assert(plan.get._1 == leftChildPlan)
    assert(plan.get._2 == rightChildPlan)
  }

  test("non supported binary node") {
    val unsupportedPlan = Intersect(leftChildPlan, rightChildPlan, isAll = true)
    val plan = BinaryOperationExtractor.unapply(unsupportedPlan)
    assert(plan.isEmpty)
  }
}
