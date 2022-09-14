package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.TestConstants._
import org.scalatest.funsuite.AnyFunSuite

class SourceQuerySuite extends AnyFunSuite{

  private val sourceQuery = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)

  test("sourceStatement") {
    assert(sourceQuery.sourceStatement.toString == "`test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS")
  }

  test("suffixStatement") {
    assert(sourceQuery.suffixStatement.toString.isEmpty)
  }

  test("columnSet") {
    assert(sourceQuery.columnSet.isEmpty)
  }

  test("processedProjections") {
    assert(sourceQuery.processedProjections.isEmpty)
  }

  test("columns") {
    assert(sourceQuery.columns.isEmpty)
  }

  test("output") {
    assert(sourceQuery.output.size == 2)
    assert(sourceQuery.output == Seq(schoolIdAttributeReference, schoolNameAttributeReference))
  }

  test("outputWithQualifier") {
    assert(sourceQuery.outputWithQualifier.size == 2)
    assert(sourceQuery.outputWithQualifier == Seq(schoolIdAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS)),
      schoolNameAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS))))
  }

  test("nullableOutputWithQualifier") {
    val nullableOutputWithQualifier = sourceQuery.nullableOutputWithQualifier
    assert(nullableOutputWithQualifier.size == 2)
    assert(nullableOutputWithQualifier == Seq(schoolIdAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS)).withNullability(true),
      schoolNameAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS)).withNullability(true)))
  }

  test("getStatement") {
    assert(sourceQuery.getStatement().toString == "SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS")
  }

  test("getStatement with alias") {
    assert(sourceQuery.getStatement(useAlias = true).toString == "( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0")
  }

  test("getStatement with pushdownFilter set") {
    val sourceQueryWithPushdownFilter = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME,
      Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS, Option.apply("studentId = 1 AND studentName = Foo"))
    assert(sourceQueryWithPushdownFilter.getStatement().toString == "SELECT * FROM " +
      "`test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS WHERE studentId = 1 AND studentName = Foo")
  }

  test("find") {
    val returnedQuery = sourceQuery.find({ case q: SourceQuery => q })
    assert(returnedQuery.isDefined)
    assert(returnedQuery.get == sourceQuery)
  }
}
