package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{expressionFactory, schoolIdAttributeReference, schoolNameAttributeReference}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, AttributeReference, Cast, EqualTo, ExprId, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Limit, LocalLimit, Project, Sort}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StringType, StructType}
import org.mockito.Mockito.when
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SparkBigQueryPushdownUtilSuite extends AnyFunSuite with BeforeAndAfter {
  @Mock
  private var directBigQueryRelationMock: DirectBigQueryRelation = _

  before {
    MockitoAnnotations.initMocks(this)
  }

  test("blockStatement with EmptyBigQuerySQLStatement") {
    val bigQuerySQLStatement = EmptyBigQuerySQLStatement.apply()
    val returnedStatement = SparkBigQueryPushdownUtil.blockStatement(bigQuerySQLStatement)
    assert("( )" == returnedStatement.toString)
  }

  test("blockStatement with BigQuerySQLStatement") {
    val bigQuerySQLStatement = ConstantString("Address") + "IS NOT NULL"
    val returnedStatement = SparkBigQueryPushdownUtil.blockStatement(bigQuerySQLStatement)
    assert("( Address IS NOT NULL )" == returnedStatement.toString)
  }

  test("blockStatement with alias") {
    val bigQuerySQLStatement = ConstantString("`bigquery-public-data.samples.shakespeare`").toStatement
    val returnedStatement = SparkBigQueryPushdownUtil.blockStatement(bigQuerySQLStatement, "bq_connector_query_alias")
    assert("( `bigquery-public-data.samples.shakespeare` ) AS BQ_CONNECTOR_QUERY_ALIAS" == returnedStatement.toString)
  }

  test("makeStatement") {
    val bigQuerySQLStatement1 = ConstantString("Name").toStatement
    val bigQuerySQLStatement2 = ConstantString("Address").toStatement
    val bigQuerySQLStatement3 = ConstantString("Email").toStatement
    val returnedStatement = SparkBigQueryPushdownUtil.makeStatement(List(bigQuerySQLStatement1, bigQuerySQLStatement2, bigQuerySQLStatement3), ",")
    assert("Name , Address , Email" == returnedStatement.toString)
  }

  test("addAttributeStatement with field match") {
    // Field match will occur on ExprId
    val attr = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
    val fields = List(AttributeReference.apply("SUBQUERY_2_COL_0", LongType)(ExprId.apply(1), List("SUBQUERY_2")),
      AttributeReference.apply("SUBQUERY_3_COL_1", LongType)(ExprId.apply(2), List("SUBQUERY_3")))
    val returnedStatement = SparkBigQueryPushdownUtil.addAttributeStatement(attr, fields)
    assert("SUBQUERY_2.SUBQUERY_2_COL_0" == returnedStatement.toString)
  }

  test("addAttributeStatement without field match") {
    // Field match will not occur since ExprId are different between attr and fields
    val attr = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
    val fields = List(AttributeReference.apply("SUBQUERY_2_COL_0", LongType)(ExprId.apply(2), List("SUBQUERY_2")),
      AttributeReference.apply("SUBQUERY_3_COL_1", LongType)(ExprId.apply(3), List("SUBQUERY_3")))
    val returnedStatement = SparkBigQueryPushdownUtil.addAttributeStatement(attr, fields)
    assert("SCHOOLID" == returnedStatement.toString)
  }

  test("renameColumns") {
    val namedExpr1 = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
    val namedExpr2 = Alias.apply(namedExpr1, "SID")(ExprId.apply(2))
    val returnedExpressions = SparkBigQueryPushdownUtil.renameColumns(List(namedExpr1, namedExpr2), "SUBQUERY_2", expressionFactory)
    assert(2 == returnedExpressions.size)
    assert("SUBQUERY_2_COL_0" == returnedExpressions.head.name)
    assert(namedExpr1.exprId == returnedExpressions.head.exprId)
    assert("SUBQUERY_2_COL_1" == returnedExpressions(1).name)
    assert(namedExpr2.exprId == returnedExpressions(1).exprId)
  }

  test("removeNodeFromPlan") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val filterPlan = Filter(EqualTo.apply(schoolIdAttributeReference, Literal(1234L)), logicalRelation)
    val projectPlan = Project(Seq(schoolNameAttributeReference), filterPlan)
    val sortPlan = Sort(Seq(SortOrder.apply(schoolIdAttributeReference, Ascending)), global = true, projectPlan)
    val limitPlan = Limit(Literal(10), sortPlan)

    val planWithProjectNodeRemoved = SparkBigQueryPushdownUtil.removeProjectNodeFromPlan(limitPlan, projectPlan)

    val expectedPlan = Limit(Literal(10),
      Sort(Seq(SortOrder.apply(schoolIdAttributeReference, Ascending)), global = true,
        Filter(EqualTo.apply(schoolIdAttributeReference, Literal(1234L)),
          logicalRelation)))

    assert(planWithProjectNodeRemoved.fastEquals(expectedPlan))
  }

  test("updateTheLogicalPlan") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val schoolIdAttributeReference: AttributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(0L))
    val filterPlan = Filter(EqualTo.apply(schoolIdAttributeReference, Literal(1234L)), logicalRelation)
    val projectPlan = Project(Seq(expressionFactory.createAlias(schoolIdAttributeReference, schoolIdAttributeReference.name, ExprId.apply(1L), Seq.empty, None)), filterPlan)
    val limitPlan = Limit(Literal(10), projectPlan)

    val planWithProjectNodeRemoved = SparkBigQueryPushdownUtil.addProjectNodeToThePlan(limitPlan, projectPlan, expressionFactory)

    assert(planWithProjectNodeRemoved.children.size == 1)
    assert(planWithProjectNodeRemoved.children(0).isInstanceOf[LocalLimit])

    // Root node of the plan is of type LocalLimit with 1 child, newly added ProjectPlan
    val actualLimitPlan = planWithProjectNodeRemoved.children(0)
    assert(actualLimitPlan.children.size == 1)
    assert(actualLimitPlan.children(0).isInstanceOf[Project])

    // The newly added Project plan has list of NamedExpressions which are cast to string
    val addedProjectPlan = actualLimitPlan.children(0).asInstanceOf[Project]
    addedProjectPlan.projectList.foreach(node => assert(node.isInstanceOf[Alias]))
    addedProjectPlan.projectList.foreach(node => assert(node.asInstanceOf[Alias].child.isInstanceOf[Cast]))
    assert(addedProjectPlan.children.size == 1)
    assert(addedProjectPlan.children(0).isInstanceOf[Project])

    // The updated project plan where cast is removed from the project list (if there is any)
    val updatedProjectPlan = addedProjectPlan.children(0).asInstanceOf[Project]
    updatedProjectPlan.projectList.foreach(node => assert(node.isInstanceOf[Alias]))
    updatedProjectPlan.projectList.foreach(node => assert(node.asInstanceOf[Alias].child.isInstanceOf[AttributeReference]))
    assert(updatedProjectPlan.children.size == 1)

    // the rest all plan should be the same
    val actualFilterPlan = updatedProjectPlan.children(0)
    assert(actualFilterPlan.fastEquals(filterPlan))
  }
}
