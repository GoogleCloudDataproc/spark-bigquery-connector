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

import com.google.cloud.bigquery.connector.common.BigQueryPushdownUnsupportedException
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, Cast, Contains, EndsWith, EqualTo, ExprId, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or, StartsWith}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SparkExpressionConverterSuite extends AnyFunSuite with BeforeAndAfter {
  private var converter: SparkExpressionConverter = _
  private val schoolIdAttributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
  private val fields = List(AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1), List("SUBQUERY_2")))

  before {
    converter = new SparkExpressionConverter {}
  }

  test("convertAggregateExpressions with COUNT") {
    val aggregateFunction = Count.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COUNT ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with DISTINCT COUNT") {
    val aggregateFunction = Count.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = true)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COUNT ( DISTINCT SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with AVERAGE") {
    val aggregateFunction = Average.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "AVG ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with CORR") {
    val aggregateFunction = Corr.apply(schoolIdAttributeReference, AttributeReference.apply("StudentID", LongType)(ExprId.apply(2)))
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CORR ( SUBQUERY_2.SCHOOLID , STUDENTID )")
  }

  test("convertAggregateExpressions with COVAR_POP") {
    val aggregateFunction = CovPopulation.apply(schoolIdAttributeReference, AttributeReference.apply("StudentID", LongType)(ExprId.apply(2)))
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COVAR_POP ( SUBQUERY_2.SCHOOLID , STUDENTID )")
  }

  test("convertAggregateExpressions with COVAR_SAMP") {
    val aggregateFunction = CovSample.apply(schoolIdAttributeReference, AttributeReference.apply("StudentID", LongType)(ExprId.apply(2)))
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COVAR_SAMP ( SUBQUERY_2.SCHOOLID , STUDENTID )")
  }

  test("convertAggregateExpressions with MAX") {
    val aggregateFunction = Max.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "MAX ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with MIN") {
    val aggregateFunction = Min.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "MIN ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with SUM") {
    val aggregateFunction = Sum.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "SUM ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with STDDEV_POP") {
    val aggregateFunction = StddevPop.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "STDDEV_POP ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with STDDEV_SAMP") {
    val aggregateFunction = StddevSamp.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "STDDEV_SAMP ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with VAR_POP") {
    val aggregateFunction = VariancePop.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "VAR_POP ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with VAR_SAMP") {
    val aggregateFunction = VarianceSamp.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "VAR_SAMP ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with non aggregate expression") {
    val nonAggregateExpression = IsNotNull.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = converter.convertAggregateExpressions(nonAggregateExpression, fields)
    assert(bigQuerySQLStatement.isEmpty)
  }

  test("convertBasicExpressions with BinaryOperator (GreaterThanOrEqual)") {
    val binaryOperatorExpression = GreaterThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(75L, LongType))
    val bigQuerySQLStatement = converter.convertBasicExpressions(binaryOperatorExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID >= 75 )")
  }

  test("convertBasicExpressions with BinaryOperator (LessThanOrEqual)") {
    val binaryOperatorExpression = LessThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(75L, LongType))
    val bigQuerySQLStatement = converter.convertBasicExpressions(binaryOperatorExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID <= 75 )")
  }

  test("convertBasicExpressions with AND") {
    val left = LessThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(100L, LongType))
    val right = GreaterThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(75L, LongType))
    val andExpression = And.apply(left, right)
    val bigQuerySQLStatement = converter.convertBasicExpressions(andExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( ( SUBQUERY_2.SCHOOLID <= 100 ) AND ( SUBQUERY_2.SCHOOLID >= 75 ) )")
  }

  test("convertBasicExpressions with OR") {
    val left = LessThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(25L, LongType))
    val right = GreaterThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(75L, LongType))
    val orExpression = Or.apply(left, right)
    val bigQuerySQLStatement = converter.convertBasicExpressions(orExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( ( SUBQUERY_2.SCHOOLID <= 25 ) OR ( SUBQUERY_2.SCHOOLID >= 75 ) )")
  }

  test("convertBasicExpressions with String literal") {
    val bigQuerySQLStatement = converter.convertBasicExpressions(Literal("MY_STRING_LITERAL"), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "'MY_STRING_LITERAL'")
  }

  test("convertBasicExpressions with null String literal") {
    val bigQuerySQLStatement = converter.convertBasicExpressions(Literal(null, StringType), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "NULL")
  }

  test("convertBasicExpressions with Integer literal") {
    val bigQuerySQLStatement = converter.convertBasicExpressions(Literal(1), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "1")
  }

  test("convertBasicExpressions with Long literal") {
    val bigQuerySQLStatement = converter.convertBasicExpressions(Literal(100L), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "100")
  }

  test("convertBasicExpressions with Short literal") {
    val bigQuerySQLStatement = converter.convertBasicExpressions(Literal(100.toShort), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "100")
  }

  test("convertBasicExpressions with Boolean literal") {
    val bigQuerySQLStatement = converter.convertBasicExpressions(Literal(false), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "false")
  }

  test("convertBasicExpressions with Float literal") {
    val bigQuerySQLStatement = converter.convertBasicExpressions(Literal(1.2345F), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "1.2345")
  }

  test("convertBasicExpressions with Double literal") {
    val bigQuerySQLStatement = converter.convertBasicExpressions(Literal(3e5D), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "300000.0")
  }

  test("convertBasicExpressions with Byte literal") {
    val bigQuerySQLStatement = converter.convertBasicExpressions(Literal(20.toByte), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "20")
  }

  test("convertBasicExpressions with non basic expression") {
    val nonBasicExpression = IsNotNull.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = converter.convertBasicExpressions(nonBasicExpression, fields)
    assert(bigQuerySQLStatement.isEmpty)
  }

  test("convertBooleanExpressions with In") {
    val inExpression = In.apply(schoolIdAttributeReference, List(Literal(100), Literal(200)))
    val bigQuerySQLStatement = converter.convertBooleanExpressions(inExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "SUBQUERY_2.SCHOOLID IN ( 100 , 200 )")
  }

  test("convertBooleanExpressions with IsNull") {
    val isNullExpression = IsNull.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = converter.convertBooleanExpressions(isNullExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID IS NULL )")
  }

  test("convertBooleanExpressions with IsNotNull") {
    val isNotNullExpression = IsNotNull.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = converter.convertBooleanExpressions(isNotNullExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID IS NOT NULL )")
  }

  test("convertBooleanExpressions with Not EqualTo") {
    val notEqualToExpression = Not.apply(EqualTo.apply(schoolIdAttributeReference, Literal(1234L)))
    val bigQuerySQLStatement = converter.convertBooleanExpressions(notEqualToExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID != 1234 )")
  }

  test("convertBooleanExpressions with Not GreaterThanOrEqual") {
    val notGreaterThanOrEqualToExpression = Not.apply(GreaterThanOrEqual.apply(schoolIdAttributeReference, Literal(1234L)))
    val bigQuerySQLStatement = converter.convertBooleanExpressions(notGreaterThanOrEqualToExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID < 1234 )")
  }

  test("convertBooleanExpressions with Not LessThanOrEqual") {
    val notLessThanOrEqualToExpression = Not.apply(LessThanOrEqual.apply(schoolIdAttributeReference, Literal(1234L)))
    val bigQuerySQLStatement = converter.convertBooleanExpressions(notLessThanOrEqualToExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID > 1234 )")
  }

  test("convertBooleanExpressions with Not GreaterThan") {
    val notGreaterThanExpression = Not.apply(GreaterThan.apply(schoolIdAttributeReference, Literal(1234L)))
    val bigQuerySQLStatement = converter.convertBooleanExpressions(notGreaterThanExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID <= 1234 )")
  }

  test("convertBooleanExpressions with Not LessThan") {
    val notLessThanExpression = Not.apply(LessThan.apply(schoolIdAttributeReference, Literal(1234L)))
    val bigQuerySQLStatement = converter.convertBooleanExpressions(notLessThanExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID >= 1234 )")
  }

  test("convertBooleanExpressions with Contains") {
    val containsExpression = Contains.apply(schoolIdAttributeReference, Literal("1234"))
    val bigQuerySQLStatement = converter.convertBooleanExpressions(containsExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CONTAINS_SUBSTR ( SUBQUERY_2.SCHOOLID , '1234%' )")
  }

  test("convertBooleanExpressions with Ends With") {
    val endsWithExpression = EndsWith.apply(schoolIdAttributeReference, Literal("1234"))
    val bigQuerySQLStatement = converter.convertBooleanExpressions(endsWithExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "ENDS_WITH ( SUBQUERY_2.SCHOOLID , '1234%' )")
  }

  test("convertBooleanExpressions with Starts With") {
    val startsWithExpression = StartsWith.apply(schoolIdAttributeReference, Literal("1234"))
    val bigQuerySQLStatement = converter.convertBooleanExpressions(startsWithExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "STARTS_WITH ( SUBQUERY_2.SCHOOLID , '1234%' )")
  }

  test("convertBooleanExpressions with non Boolean expression") {
    val bigQuerySQLStatement = converter.convertBooleanExpressions(Literal(100L), fields)
    assert(bigQuerySQLStatement.isEmpty)
  }

  test("convertMiscExpressions with Alias") {
    val aliasExpression = Alias.apply(schoolIdAttributeReference, "SCHOOL_ID_ALIAS")(ExprId.apply(1))
    val bigQuerySQLStatement = converter.convertMiscExpressions(aliasExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID ) AS SCHOOL_ID_ALIAS")
  }

  test("convertMiscExpressions with unsupported casts") {
    assertThrows[BigQueryPushdownUnsupportedException] {
      converter.convertMiscExpressions(Cast.apply(AttributeReference.apply("Date", DateType)(ExprId.apply(2)), IntegerType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      converter.convertMiscExpressions(Cast.apply(AttributeReference.apply("Date", DateType)(ExprId.apply(2)), LongType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      converter.convertMiscExpressions(Cast.apply(AttributeReference.apply("Date", DateType)(ExprId.apply(2)), FloatType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      converter.convertMiscExpressions(Cast.apply(AttributeReference.apply("Date", DateType)(ExprId.apply(2)), DoubleType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      converter.convertMiscExpressions(Cast.apply(AttributeReference.apply("Date", DateType)(ExprId.apply(2)), DecimalType(10, 0)), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      converter.convertMiscExpressions(Cast.apply(AttributeReference.apply("Timestamp", TimestampType)(ExprId.apply(2)), IntegerType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      converter.convertMiscExpressions(Cast.apply(AttributeReference.apply("Timestamp", TimestampType)(ExprId.apply(2)), LongType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      converter.convertMiscExpressions(Cast.apply(AttributeReference.apply("Timestamp", TimestampType)(ExprId.apply(2)), FloatType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      converter.convertMiscExpressions(Cast.apply(AttributeReference.apply("Timestamp", TimestampType)(ExprId.apply(2)), DoubleType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      converter.convertMiscExpressions(Cast.apply(AttributeReference.apply("Timestamp", TimestampType)(ExprId.apply(2)), DecimalType(10, 0)), fields)
    }
  }

  test("convertMiscExpressions with Cast from Integer to String") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", IntegerType)(ExprId.apply(2)), StringType)
    val bigQuerySQLStatement = converter.convertMiscExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS STRING )")
  }

  test("convertMiscExpressions with Cast from Integer to Boolean") {
    val castExpression = Cast.apply(AttributeReference.apply("is_student", IntegerType)(ExprId.apply(2)), BooleanType)
    val bigQuerySQLStatement = converter.convertMiscExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( IS_STUDENT AS BOOL )")
  }

  test("convertMiscExpressions with Cast from Integer to Short") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", IntegerType)(ExprId.apply(2)), ShortType)
    val bigQuerySQLStatement = converter.convertMiscExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS INT64 )")
  }

  test("convertMiscExpressions with Cast from Integer to Long") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", IntegerType)(ExprId.apply(2)), LongType)
    val bigQuerySQLStatement = converter.convertMiscExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS INT64 )")
  }

  test("convertMiscExpressions with Cast from Long to Integer") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", LongType)(ExprId.apply(2)), IntegerType)
    val bigQuerySQLStatement = converter.convertMiscExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS INT64 )")
  }

  test("convertMiscExpressions with Cast from Integer to Float") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", IntegerType)(ExprId.apply(2)), FloatType)
    val bigQuerySQLStatement = converter.convertMiscExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS FLOAT64 )")
  }

  test("convertMiscExpressions with Cast from Integer to Double") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", IntegerType)(ExprId.apply(2)), DoubleType)
    val bigQuerySQLStatement = converter.convertMiscExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS FLOAT64 )")
  }

  test("convertMiscExpressions with Cast from String to Date") {
    val castExpression = Cast.apply(AttributeReference.apply("attendance_date", StringType)(ExprId.apply(2)), DateType)
    val bigQuerySQLStatement = converter.convertMiscExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( ATTENDANCE_DATE AS DATE )")
  }

  test("convertMiscExpressions with Cast from String to Timestamp") {
    val castExpression = Cast.apply(AttributeReference.apply("last_modified", StringType)(ExprId.apply(2)), TimestampType)
    val bigQuerySQLStatement = converter.convertMiscExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( LAST_MODIFIED AS TIMESTAMP )")
  }

  test("convertMiscExpressions with Cast from String to Bytes") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", StringType)(ExprId.apply(2)), ByteType)
    val bigQuerySQLStatement = converter.convertMiscExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS BYTES )")
  }

  test("convertMiscExpressions with Cast from String to BigDecimal/BigNumeric") {
    val castExpression = Cast.apply(AttributeReference.apply("Transaction", StringType)(ExprId.apply(2)), DecimalType(10, 5))
    val bigQuerySQLStatement = converter.convertMiscExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( TRANSACTION AS BIGDECIMAL(10, 5) )")
  }
}
