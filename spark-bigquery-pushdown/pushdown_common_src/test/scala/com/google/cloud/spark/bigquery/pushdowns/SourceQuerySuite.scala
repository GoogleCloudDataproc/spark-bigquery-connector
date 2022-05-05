package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.funsuite.AnyFunSuite

class SourceQuerySuite extends AnyFunSuite{
  private val TABLE_NAME = "test_project:test_dataset.test_table"
  private val ALIAS = "SUBQUERY_0"

  private val expressionConverter = new SparkExpressionConverter {}
  private val schoolIdAttributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
  private val lastNameAttributeReference = AttributeReference.apply("LastName", StringType)(ExprId.apply(2))

  private val sourceQuery = SourceQuery(expressionConverter, TABLE_NAME,
    Seq(schoolIdAttributeReference, lastNameAttributeReference), ALIAS)

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
    assert(sourceQuery.output == Seq(schoolIdAttributeReference, lastNameAttributeReference))
  }

  test("outputWithQualifier") {
    assert(sourceQuery.outputWithQualifier.size == 2)
    assert(sourceQuery.outputWithQualifier == Seq(schoolIdAttributeReference.withQualifier(Seq(ALIAS)),
      lastNameAttributeReference.withQualifier(Seq(ALIAS))))
  }

  test("getStatement") {
    assert(sourceQuery.getStatement().toString == "SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS")
  }

  test("getStatement with alias") {
    assert(sourceQuery.getStatement(useAlias = true).toString == "( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0")
  }
}
