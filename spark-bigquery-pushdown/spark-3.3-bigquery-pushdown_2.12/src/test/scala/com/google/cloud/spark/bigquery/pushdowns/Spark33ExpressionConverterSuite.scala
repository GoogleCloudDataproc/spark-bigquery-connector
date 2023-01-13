/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, CheckOverflow, ExprId, Like, Literal, ScalarSubquery, UnaryMinus}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{DecimalType, LongType, StringType, StructType, TimestampType}
import org.mockito.Mockito.when
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class Spark33ExpressionConverterSuite extends AnyFunSuite with BeforeAndAfter {
  @Mock
  var directBigQueryRelationMock: DirectBigQueryRelation = _
  @Mock
  var sparkPlanFactoryMock: SparkPlanFactory = _

  private val expressionFactory = new Spark33ExpressionFactory
  private val expressionConverter = new Spark33ExpressionConverter(expressionFactory, sparkPlanFactoryMock)
  private val fields = List(AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1), List("SUBQUERY_2")))

  before {
    MockitoAnnotations.initMocks(this)
  }

  test("convertMiscellaneousExpressions with ScalarSubquery") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")
    val logicalRelation = LogicalRelation(directBigQueryRelationMock)
    val aggregatePlan = Aggregate(Seq(), Seq(), logicalRelation)
    val scalarSubQueryExpression = ScalarSubquery.apply(aggregatePlan)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(scalarSubQueryExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SELECT * FROM ( SELECT * FROM `MY_BIGQUERY_TABLE` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 LIMIT 1 )")
  }

  test("convertMathematicalExpressions with UnaryMinus") {
    val unaryMinusExpression = UnaryMinus.apply(Literal.apply(10))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(unaryMinusExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "- ( 10 )")
  }

  test("convertMathematicalExpressions with CheckOverflow") {
    val checkOverflowExpression = CheckOverflow.apply(Literal.apply(233.45), DecimalType.apply(38, 10), nullOnOverflow = true)
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(checkOverflowExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( 233.45 AS BIGDECIMAL )")
  }

  test("convertBasicExpressions with Timestamp literal") {
    // Internally, a timestamp is stored as the number of microseconds from the epoch of 1970-01-01T00
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal(1230219000000000L, TimestampType), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "TIMESTAMP_MICROS( 1230219000000000 )")
  }

  test("convertStringExpressions with Like") {
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(
      Like(AttributeReference.apply("SchoolID", StringType)(ExprId.apply(1)),
        Literal("%aug%urs%"), '\\'), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "SUBQUERY_2.SCHOOLID LIKE '%aug%urs%'")
  }
}
