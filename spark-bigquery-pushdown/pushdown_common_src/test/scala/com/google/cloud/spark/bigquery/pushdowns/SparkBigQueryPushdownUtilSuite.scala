package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{expressionFactory, schoolIdAttributeReference, schoolNameAttributeReference}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, AttributeReference, EqualTo, ExprId, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Limit, Project, Sort}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{LongType, StructType}
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
}
