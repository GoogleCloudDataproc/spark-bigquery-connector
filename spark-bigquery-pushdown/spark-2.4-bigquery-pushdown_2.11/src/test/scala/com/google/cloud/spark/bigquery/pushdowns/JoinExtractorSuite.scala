package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, Range}
import org.scalatest.funsuite.AnyFunSuite

class JoinExtractorSuite extends AnyFunSuite {
  // Need a childPlan to pass. So, create the simplest possible
  private val leftChildPlan = Range.apply(2L, 100L, 4L, 8)

  // Need a childPlan to pass. So, create the simplest possible
  private val rightChildPlan = Range.apply(2L, 100L, 4L, 16)

  test("unapply") {
    val joinExpression = EqualTo.apply(Literal(leftChildPlan.start), Literal(rightChildPlan.start))
    val joinPlan = Join.apply(leftChildPlan, rightChildPlan, JoinType.apply("inner"), Option(joinExpression))
    val returnedOption = JoinExtractor.unapply(joinPlan)
    assert(returnedOption.isDefined)
    assert(returnedOption.get._1 == JoinType.apply("inner"))
    assert(returnedOption.get._2 == Option(joinExpression))
  }
}
