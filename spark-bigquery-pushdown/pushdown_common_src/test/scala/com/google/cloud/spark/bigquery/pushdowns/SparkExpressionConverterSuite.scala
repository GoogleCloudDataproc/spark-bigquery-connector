package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count, Sum}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, ExprId, GreaterThanOrEqual, IsNotNull, Literal}
import org.apache.spark.sql.types.LongType
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SparkExpressionConverterSuite extends AnyFunSuite with BeforeAndAfter {
  private var converter: SparkExpressionConverter = _
  private val fields = List(AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1), List("SUBQUERY_2")))

  before {
    converter = new SparkExpressionConverter {}
  }

  test("convertAggregateExpressions with COUNT") {
    val aggregateFunction = Count.apply(AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1)))
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COUNT ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with DISTINCT COUNT") {
    val aggregateFunction = Count.apply(AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1)))
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = true)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COUNT ( DISTINCT SUBQUERY_2.SCHOOLID )")
  }

  test("convertBasicExpressions with AND") {
    val attributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
    val left = IsNotNull.apply(attributeReference)
    val right = GreaterThanOrEqual.apply(attributeReference, Literal.apply(75L, LongType))
    val andExpression = And.apply(left, right)
    val bigQuerySQLStatement = converter.convertBasicExpressions(andExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( ( SUBQUERY_2.SCHOOLID IS NOT NULL ) AND ( SUBQUERY_2.SCHOOLID >= 75 ) )")
  }

  test("convertBooleanExpressions with IsNotNull") {
    val attributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
    val isNotNullExpression = IsNotNull.apply(attributeReference)
    val bigQuerySQLStatement = converter.convertBooleanExpressions(isNotNullExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID IS NOT NULL )")
  }

  test("convertMiscExpressions with Alias") {
    val attributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
    val aliasExpression = Alias.apply(attributeReference, "SCHOOL_ID_ALIAS")(ExprId.apply(1))
    val bigQuerySQLStatement = converter.convertMiscExpressions(aliasExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID ) AS SCHOOL_ID_ALIAS")
  }
}
