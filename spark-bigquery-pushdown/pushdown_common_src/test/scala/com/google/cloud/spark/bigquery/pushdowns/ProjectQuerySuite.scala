package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.TestConstants._
import org.apache.spark.sql.catalyst.expressions.{Alias, GreaterThanOrEqual, LessThanOrEqual, Literal}
import org.scalatest.funsuite.AnyFunSuite

class ProjectQuerySuite extends AnyFunSuite{

  private val sourceQuery = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)

  // Conditions for filter query (> 50 AND < 100)
  private val greaterThanFilterCondition = GreaterThanOrEqual.apply(schoolIdAttributeReference, Literal(50))
  private val lessThanFilterCondition = LessThanOrEqual.apply(schoolIdAttributeReference, Literal(100))
  private val filterQuery = FilterQuery(expressionConverter, expressionFactory, Seq(greaterThanFilterCondition, lessThanFilterCondition), sourceQuery, SUBQUERY_1_ALIAS)

  // Projecting the column SchoolId
  private val projectQuery = ProjectQuery(expressionConverter, expressionFactory, Seq(schoolIdAttributeReference), filterQuery, SUBQUERY_2_ALIAS)

  test("sourceStatement") {
    assert(projectQuery.sourceStatement.toString == "( SELECT * FROM ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 " +
      "WHERE ( SUBQUERY_0.SCHOOLID >= 50 ) AND ( SUBQUERY_0.SCHOOLID <= 100 ) ) AS SUBQUERY_1")
  }

  test("suffixStatement") {
    assert(projectQuery.suffixStatement.isEmpty)
  }

  test("columnSet") {
    assert(projectQuery.columnSet.size == 2)
    assert(projectQuery.columnSet == Seq(schoolIdAttributeReference.withQualifier(Seq(SUBQUERY_1_ALIAS)),
      schoolNameAttributeReference.withQualifier(Seq(SUBQUERY_1_ALIAS))))
  }

  test("processedProjections") {
    assert(projectQuery.processedProjections.isDefined)
    assert(projectQuery.processedProjections.get == Seq(Alias(schoolIdAttributeReference, SUBQUERY_2_ALIAS + "_COL_0")(schoolIdAttributeReference.exprId,
      schoolIdAttributeReference.qualifier, Some(schoolIdAttributeReference.metadata))))
  }

  test("columns") {
    assert(projectQuery.columns.isDefined)
    assert(projectQuery.columns.get.toString == "( SUBQUERY_1.SCHOOLID ) AS SUBQUERY_2_COL_0")
  }

  test("output") {
    assert(projectQuery.output.size == 1)
    assert(projectQuery.output == Seq(schoolIdAttributeReference.withName("SUBQUERY_2_COL_0")))
  }

  test("outputWithQualifier") {
    assert(projectQuery.outputWithQualifier.size == 1)
    assert(projectQuery.outputWithQualifier == Seq(schoolIdAttributeReference.withName(SUBQUERY_2_ALIAS + "_COL_0").withQualifier(Seq(SUBQUERY_2_ALIAS))))
  }

  test("getStatement") {
    assert(projectQuery.getStatement().toString == "SELECT ( SUBQUERY_1.SCHOOLID ) AS SUBQUERY_2_COL_0 FROM " +
      "( SELECT * FROM ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 " +
      "WHERE ( SUBQUERY_0.SCHOOLID >= 50 ) AND ( SUBQUERY_0.SCHOOLID <= 100 ) ) AS SUBQUERY_1")
  }

  test("getStatement with alias") {
    assert(projectQuery.getStatement(useAlias = true).toString == "( SELECT ( SUBQUERY_1.SCHOOLID ) AS SUBQUERY_2_COL_0 FROM " +
      "( SELECT * FROM ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 " +
      "WHERE ( SUBQUERY_0.SCHOOLID >= 50 ) AND ( SUBQUERY_0.SCHOOLID <= 100 ) ) AS SUBQUERY_1 ) AS SUBQUERY_2")
  }

  test("find") {
    val returnedQuery = projectQuery.find({ case q: SourceQuery => q })
    assert(returnedQuery.isDefined)
    assert(returnedQuery.get == sourceQuery)
  }
}
