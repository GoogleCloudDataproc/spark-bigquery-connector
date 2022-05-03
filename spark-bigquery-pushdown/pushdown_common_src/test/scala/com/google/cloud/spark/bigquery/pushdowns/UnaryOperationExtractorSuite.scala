package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, ExprId, Literal}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, Join, LocalLimit, LogicalPlan, Project, Range, ReturnAnswer, Sort, SubqueryAlias, Window}
import org.apache.spark.sql.types.LongType
import org.scalatest.funsuite.AnyFunSuite

class UnaryOperationExtractorSuite extends AnyFunSuite {
  private val schoolIdAttributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))

  // Need a childPlan to pass. So, create the simplest possible
  private val childPlan = Range.apply(2L, 100L, 4L, 8)

  test("Filter") {
    val filterExpression = EqualTo.apply(schoolIdAttributeReference, Literal(1234L))
    val filterPlan = Filter(filterExpression, childPlan)
    val plan = UnaryOperationExtractor.unapply(filterPlan)
    assertReturnedPlan(plan)
  }

  test("Project") {
    val projectPlan = Project(Seq(), childPlan)
    val plan = UnaryOperationExtractor.unapply(projectPlan)
    assertReturnedPlan(plan)
  }

  test("GlobalLimit") {
    val globalLimitPlan = GlobalLimit(Literal(21), childPlan)
    val plan = UnaryOperationExtractor.unapply(globalLimitPlan)
    assertReturnedPlan(plan)
  }

  test("LocalLimit") {
    val localLimitPlan = LocalLimit(Literal(21), childPlan)
    val plan = UnaryOperationExtractor.unapply(localLimitPlan)
    assertReturnedPlan(plan)
  }

  test("Aggregate") {
    val aggregatePlan = Aggregate(Seq(), Seq() , childPlan)
    val plan = UnaryOperationExtractor.unapply(aggregatePlan)
    assertReturnedPlan(plan)
  }

  test("Sort") {
    val sortPlan = Sort(Seq(), global = false, childPlan)
    val plan = UnaryOperationExtractor.unapply(sortPlan)
    assertReturnedPlan(plan)
  }

  test("ReturnAnswer") {
    val returnAnswerPlan = ReturnAnswer(childPlan)
    val plan = UnaryOperationExtractor.unapply(returnAnswerPlan)
    assertReturnedPlan(plan)
  }

  test("Window") {
    val windowPlan = Window(Seq(), Seq(), Seq(), childPlan)
    val plan = UnaryOperationExtractor.unapply(windowPlan)
    assertReturnedPlan(plan)
  }

  test("non supported unary node") {
    val subqueryAliasPlan = SubqueryAlias("subquery_1", childPlan)
    val plan = UnaryOperationExtractor.unapply(subqueryAliasPlan)
    assert(plan.isEmpty)
  }

  def assertReturnedPlan(plan: Option[LogicalPlan]): Unit = {
    assert(plan.isDefined)
    assert(plan.get.isInstanceOf[Range])
    assert(plan.get == childPlan)
  }

}
