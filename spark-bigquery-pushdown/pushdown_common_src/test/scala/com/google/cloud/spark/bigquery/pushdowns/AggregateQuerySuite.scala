package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{SUBQUERY_0_ALIAS, TABLE_NAME, expressionConverter, schoolIdAttributeReference, schoolNameAttributeReference}
import org.scalatest.funsuite.AnyFunSuite

// Testing only the suffixStatement here since it is the only variable that is
// different from the other queries.
class AggregateQuerySuite extends AnyFunSuite{
  private val sourceQuery = SourceQuery(expressionConverter, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)

  test("suffixStatement with groups") {
    // Passing projectionColumns parameter as empty list for simplicity
    val aggregateQuery = AggregateQuery(expressionConverter, projectionColumns = Seq(), groups = Seq(schoolNameAttributeReference), sourceQuery, SUBQUERY_0_ALIAS)
    assert(aggregateQuery.suffixStatement.toString == "GROUP BY SUBQUERY_0.SCHOOLNAME")
  }

  test("suffixStatement without groups") {
    // Passing projectionColumns parameter as empty list for simplicity
    val aggregateQuery = AggregateQuery(expressionConverter, projectionColumns = Seq(), groups = Seq(), sourceQuery, SUBQUERY_0_ALIAS)
    assert(aggregateQuery.suffixStatement.toString == "LIMIT 1")
  }
}
