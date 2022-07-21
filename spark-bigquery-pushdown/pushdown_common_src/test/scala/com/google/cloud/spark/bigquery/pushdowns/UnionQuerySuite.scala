package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{SUBQUERY_0_ALIAS, SUBQUERY_1_ALIAS, SUBQUERY_2_ALIAS, TABLE_NAME, bigQueryRDDFactoryMock, expressionConverter, expressionFactory, schoolIdAttributeReference, schoolNameAttributeReference}
import org.scalatest.funsuite.AnyFunSuite

class UnionQuerySuite extends AnyFunSuite{

  private val sourceQuery1 = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)
  private val sourceQuery2 = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_1_ALIAS)

  test(testName = "getStatement with nonempty children and useAlias true") {
    val unionQuery = UnionQuery(expressionConverter, expressionFactory, Seq(sourceQuery1, sourceQuery2), SUBQUERY_2_ALIAS)
    val bigQuerySQLStatement = unionQuery.getStatement(true)
    assert(bigQuerySQLStatement.toString == "( ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) UNION ALL ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) ) AS SUBQUERY_2")
  }

  test(testName = "getStatement with nonempty children and useAlias false") {
    val unionQuery = UnionQuery(expressionConverter, expressionFactory, Seq(sourceQuery1, sourceQuery2), SUBQUERY_2_ALIAS)
    val bigQuerySQLStatement = unionQuery.getStatement()
    assert(bigQuerySQLStatement.toString == "( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) UNION ALL ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS )")
  }

  test("find") {
    val unionQuery = UnionQuery(expressionConverter, expressionFactory, Seq(sourceQuery1, sourceQuery2), SUBQUERY_2_ALIAS)
    val returnedQuery = unionQuery.find({ case q: SourceQuery => q })
    assert(returnedQuery.isDefined)
    assert(returnedQuery.get == sourceQuery1)
  }
}
