package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.TestConstants._
import org.apache.spark.sql.catalyst.expressions.{GreaterThanOrEqual, LessThanOrEqual, Literal}
import org.scalatest.funsuite.AnyFunSuite

class FilterQuerySuite extends AnyFunSuite {

  private val sourceQuery = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)

  private val greaterThanFilterCondition = GreaterThanOrEqual.apply(schoolIdAttributeReference, Literal(50))
  private val lessThanFilterCondition = LessThanOrEqual.apply(schoolIdAttributeReference, Literal(100))
  private val filterQuery = FilterQuery(expressionConverter, expressionFactory, Seq(greaterThanFilterCondition, lessThanFilterCondition), sourceQuery, SUBQUERY_1_ALIAS)

  test("sourceStatement") {
    assert(filterQuery.sourceStatement.toString == "( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0")
  }

  test("suffixStatement") {
    assert(filterQuery.suffixStatement.toString == "WHERE ( SUBQUERY_0.SCHOOLID >= 50 ) AND ( SUBQUERY_0.SCHOOLID <= 100 )")
  }

  test("columnSet") {
    assert(filterQuery.columnSet.size == 2)
    assert(filterQuery.columnSet == Seq(schoolIdAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS)),
      schoolNameAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS))))
  }

  test("processedProjections") {
    assert(filterQuery.processedProjections.isEmpty)
  }

  test("columns") {
    assert(filterQuery.columns.isEmpty)
  }

  test("output") {
    assert(filterQuery.output.size == 2)
    assert(filterQuery.output == Seq(schoolIdAttributeReference, schoolNameAttributeReference))
  }

  test("outputWithQualifier") {
    assert(filterQuery.outputWithQualifier.size == 2)
    assert(filterQuery.outputWithQualifier == Seq(schoolIdAttributeReference.withQualifier(Seq(SUBQUERY_1_ALIAS)),
      schoolNameAttributeReference.withQualifier(Seq(SUBQUERY_1_ALIAS))))
  }

  test("nullableOutputWithQualifier") {
    val nullableOutputWithQualifier = filterQuery.nullableOutputWithQualifier
    assert(nullableOutputWithQualifier.size == 2)
    assert(nullableOutputWithQualifier == Seq(schoolIdAttributeReference.withQualifier(Seq(SUBQUERY_1_ALIAS)).withNullability(true),
      schoolNameAttributeReference.withQualifier(Seq(SUBQUERY_1_ALIAS)).withNullability(true)))
  }

  test("getStatement") {
    assert(filterQuery.getStatement().toString == "SELECT * FROM ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 " +
      "WHERE ( SUBQUERY_0.SCHOOLID >= 50 ) AND ( SUBQUERY_0.SCHOOLID <= 100 )")
  }

  test("getStatement with alias") {
    assert(filterQuery.getStatement(useAlias = true).toString == "( SELECT * FROM ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) " +
      "AS SUBQUERY_0 WHERE ( SUBQUERY_0.SCHOOLID >= 50 ) AND ( SUBQUERY_0.SCHOOLID <= 100 ) ) AS SUBQUERY_1")
  }

  test("find") {
    val returnedQuery = filterQuery.find({ case q: SourceQuery => q })
    assert(returnedQuery.isDefined)
    assert(returnedQuery.get == sourceQuery)
  }
}
