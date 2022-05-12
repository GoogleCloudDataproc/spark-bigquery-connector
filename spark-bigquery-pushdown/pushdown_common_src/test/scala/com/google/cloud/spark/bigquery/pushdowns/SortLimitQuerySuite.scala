package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{SUBQUERY_0_ALIAS, SUBQUERY_1_ALIAS, TABLE_NAME, expressionConverter, expressionFactory, schoolIdAttributeReference, schoolNameAttributeReference}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Literal, SortOrder}
import org.scalatest.funsuite.AnyFunSuite

// Testing only the suffixStatement here since it is the only variable that is
// different from the other queries.
class SortLimitQuerySuite extends AnyFunSuite{
  private val sourceQuery = SourceQuery(expressionConverter, expressionFactory, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)

  test("suffixStatement with only limit") {
    val sortLimitQuery = SortLimitQuery(expressionConverter, expressionFactory, Some(Literal(10)), Seq(), sourceQuery, SUBQUERY_1_ALIAS)
    assert(sortLimitQuery.suffixStatement.toString == "LIMIT 10")
  }

  test("suffixStatement with only orderBy") {
    val sortOrder = SortOrder.apply(schoolIdAttributeReference, Ascending)
    val sortLimitQuery = SortLimitQuery(expressionConverter, expressionFactory, None, Seq(sortOrder), sourceQuery, SUBQUERY_1_ALIAS)
    assert(sortLimitQuery.suffixStatement.toString == "ORDER BY ( SUBQUERY_0.SCHOOLID ) ASC")
  }

  test("suffixStatement with both orderBy and limit") {
    val sortOrder = SortOrder.apply(schoolIdAttributeReference, Ascending)
    val sortLimitQuery = SortLimitQuery(expressionConverter, expressionFactory, Some(Literal(10)), Seq(sortOrder), sourceQuery, SUBQUERY_1_ALIAS)
    assert(sortLimitQuery.suffixStatement.toString == "ORDER BY ( SUBQUERY_0.SCHOOLID ) ASC LIMIT 10")
  }
}
