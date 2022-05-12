package com.google.cloud.spark.bigquery.pushdowns

<<<<<<< HEAD
import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{SUBQUERY_0_ALIAS, TABLE_NAME, bigQueryRDDFactoryMock, expressionConverter, schoolIdAttributeReference, schoolNameAttributeReference}
=======
import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{SUBQUERY_0_ALIAS, TABLE_NAME, expressionConverter, expressionFactory, schoolIdAttributeReference, schoolNameAttributeReference}
>>>>>>> master
import org.scalatest.funsuite.AnyFunSuite

// Testing only the suffixStatement here since it is the only variable that is
// different from the other queries.
class AggregateQuerySuite extends AnyFunSuite{
<<<<<<< HEAD
  private val sourceQuery = SourceQuery(expressionConverter, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)
=======
  private val sourceQuery = SourceQuery(expressionConverter, expressionFactory, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)
>>>>>>> master

  test("suffixStatement with groups") {
    // Passing projectionColumns parameter as empty list for simplicity
    val aggregateQuery = AggregateQuery(expressionConverter, expressionFactory, projectionColumns = Seq(), groups = Seq(schoolNameAttributeReference), sourceQuery, SUBQUERY_0_ALIAS)
    assert(aggregateQuery.suffixStatement.toString == "GROUP BY SUBQUERY_0.SCHOOLNAME")
  }

  test("suffixStatement without groups") {
    // Passing projectionColumns parameter as empty list for simplicity
    val aggregateQuery = AggregateQuery(expressionConverter, expressionFactory, projectionColumns = Seq(), groups = Seq(), sourceQuery, SUBQUERY_0_ALIAS)
    assert(aggregateQuery.suffixStatement.toString == "LIMIT 1")
  }

  test("find") {
    val aggregateQuery = AggregateQuery(expressionConverter, projectionColumns = Seq(), groups = Seq(), sourceQuery, SUBQUERY_0_ALIAS)
    val returnedQuery = aggregateQuery.find({ case q: SourceQuery => q })
    assert(returnedQuery.isDefined)
    assert(returnedQuery.get == sourceQuery)
  }
}
