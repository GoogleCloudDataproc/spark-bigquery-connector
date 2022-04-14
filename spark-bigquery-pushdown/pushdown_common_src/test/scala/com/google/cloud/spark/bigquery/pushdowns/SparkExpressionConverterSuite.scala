/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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
