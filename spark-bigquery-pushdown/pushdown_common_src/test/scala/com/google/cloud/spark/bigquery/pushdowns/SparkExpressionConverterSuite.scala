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
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import com.google.cloud.spark.bigquery.pushdowns.TestConstants.expressionConverter
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Abs, Acos, Alias, And, Ascending, Ascii, Asin, Atan, AttributeReference, Base64, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, CaseWhen, Cast, Coalesce, Concat, Contains, Cos, Cosh, DateAdd, DateSub, DenseRank, Descending, EndsWith, EqualNullSafe, EqualTo, Exp, ExprId, Floor, FormatNumber, FormatString, GreaterThan, GreaterThanOrEqual, Greatest, If, In, InSet, InitCap, IsNaN, IsNotNull, IsNull, Least, Length, LessThan, LessThanOrEqual, Literal, Log10, Logarithm, Lower, Month, Not, Or, PercentRank, Pi, Pow, PromotePrecision, Quarter, Rand, Rank, RegExpExtract, RegExpReplace, Round, RowNumber, ShiftLeft, ShiftRight, Signum, Sin, Sinh, SortOrder, SoundEx, Sqrt, StartsWith, StringInstr, StringLPad, StringRPad, StringTranslate, StringTrim, StringTrimLeft, StringTrimRight, Substring, Tan, Tanh, TruncDate, UnBase64, UnscaledValue, Upper, Year}
import org.apache.spark.sql.types._
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SparkExpressionConverterSuite extends AnyFunSuite with BeforeAndAfter {
  private val schoolIdAttributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
  private val schoolStartDateAttributeReference = AttributeReference.apply("StartDate", DateType)(ExprId.apply(2))
  private val fields = List(AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1), List("SUBQUERY_2")))
  @Mock
  var directBigQueryRelationMock: DirectBigQueryRelation = _

  before {
    MockitoAnnotations.initMocks(this)
  }

  test("convertAggregateExpressions with COUNT") {
    val aggregateFunction = Count.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COUNT ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with DISTINCT COUNT") {
    val aggregateFunction = Count.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = true)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COUNT ( DISTINCT SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with AVERAGE") {
    val aggregateFunction = Average.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "AVG ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with CORR") {
    val aggregateFunction = Corr.apply(schoolIdAttributeReference, AttributeReference.apply("StudentID", LongType)(ExprId.apply(2)))
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CORR ( SUBQUERY_2.SCHOOLID , STUDENTID )")
  }

  test("convertAggregateExpressions with COVAR_POP") {
    val aggregateFunction = CovPopulation.apply(schoolIdAttributeReference, AttributeReference.apply("StudentID", LongType)(ExprId.apply(2)))
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COVAR_POP ( SUBQUERY_2.SCHOOLID , STUDENTID )")
  }

  test("convertAggregateExpressions with COVAR_SAMP") {
    val aggregateFunction = CovSample.apply(schoolIdAttributeReference, AttributeReference.apply("StudentID", LongType)(ExprId.apply(2)))
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COVAR_SAMP ( SUBQUERY_2.SCHOOLID , STUDENTID )")
  }

  test("convertAggregateExpressions with MAX") {
    val aggregateFunction = Max.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "MAX ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with MIN") {
    val aggregateFunction = Min.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "MIN ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with SUM") {
    val aggregateFunction = Sum.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "SUM ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with STDDEV_POP") {
    val aggregateFunction = StddevPop.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "STDDEV_POP ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with STDDEV_SAMP") {
    val aggregateFunction = StddevSamp.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "STDDEV_SAMP ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with VAR_POP") {
    val aggregateFunction = VariancePop.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "VAR_POP ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with VAR_SAMP") {
    val aggregateFunction = VarianceSamp.apply(schoolIdAttributeReference)
    val aggregateExpression = AggregateExpression.apply(aggregateFunction, Complete, isDistinct = false)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(aggregateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "VAR_SAMP ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertAggregateExpressions with non aggregate expression") {
    val nonAggregateExpression = IsNotNull.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertAggregateExpressions(nonAggregateExpression, fields)
    assert(bigQuerySQLStatement.isEmpty)
  }

  test("convertBasicExpressions with BinaryOperator (GreaterThanOrEqual)") {
    val binaryOperatorExpression = GreaterThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(75L, LongType))
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(binaryOperatorExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID >= 75 )")
  }

  test("convertBasicExpressions with BinaryOperator (LessThanOrEqual)") {
    val binaryOperatorExpression = LessThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(75L, LongType))
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(binaryOperatorExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID <= 75 )")
  }

  test("convertBasicExpressions with AND") {
    val left = LessThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(100L, LongType))
    val right = GreaterThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(75L, LongType))
    val andExpression = And.apply(left, right)
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(andExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( ( SUBQUERY_2.SCHOOLID <= 100 ) AND ( SUBQUERY_2.SCHOOLID >= 75 ) )")
  }

  test("convertBasicExpressions with OR") {
    val left = LessThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(25L, LongType))
    val right = GreaterThanOrEqual.apply(schoolIdAttributeReference, Literal.apply(75L, LongType))
    val orExpression = Or.apply(left, right)
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(orExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( ( SUBQUERY_2.SCHOOLID <= 25 ) OR ( SUBQUERY_2.SCHOOLID >= 75 ) )")
  }

  test("convertBasicExpressions with BitwiseAnd") {
    val left = Literal.apply(0)
    val right = Literal.apply(1)
    val bitwiseAndExpression = BitwiseAnd.apply(left, right)
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(bitwiseAndExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( 0 & 1 )")
  }

  test("convertBasicExpressions with BitwiseOr") {
    val left = Literal.apply(0)
    val right = Literal.apply(1)
    val bitwiseOrExpression = BitwiseOr.apply(left, right)
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(bitwiseOrExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( 0 | 1 )")
  }

  test("convertBasicExpressions with BitwiseXor") {
    val left = Literal.apply(0)
    val right = Literal.apply(1)
    val bitwiseXorExpression = BitwiseXor.apply(left, right)
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(bitwiseXorExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( 0 ^ 1 )")
  }

  test("convertBasicExpressions with BitwiseNot") {
    val child = Literal.apply(1)
    val bitwiseXorExpression = BitwiseNot.apply(child)
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(bitwiseXorExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "~ ( 1 )")
  }

  test("convertBasicExpressions with EqualNullSafe") {
    val left = Literal.apply(1)
    val right = Literal.apply(null)
    val equalNullSafeExpression = EqualNullSafe.apply(left, right)
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(equalNullSafeExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( COALESCE ( CAST ( 1 AS STRING ) , \"\" ) = COALESCE ( CAST ( NULL AS STRING ) , \"\" ) )")
  }

  test("convertBasicExpressions with String literal") {
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal("MY_STRING_LITERAL"), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "'MY_STRING_LITERAL'")
  }

  test("convertBasicExpressions with null String literal") {
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal(null, StringType), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "NULL")
  }

  test("convertBasicExpressions with Date literal") {
    // Spark represents DateType as number of days after 1970-01-01
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal(17007, DateType), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "DATE_ADD(DATE \"1970-01-01\", INTERVAL 17007  DAY)")
  }

  test("convertBasicExpressions with Timestamp literal") {
    // Internally, a timestamp is stored as the number of microseconds from the epoch of 1970-01-01T00
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal(1230219000000000L, TimestampType), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "TIMESTAMP_MICROS( 1230219000000000 )")
  }

  test("convertBasicExpressions with Integer literal") {
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal(1), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "1")
  }

  test("convertBasicExpressions with Long literal") {
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal(100L), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "100")
  }

  test("convertBasicExpressions with Short literal") {
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal(100.toShort), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "100")
  }

  test("convertBasicExpressions with Boolean literal") {
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal(false), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "false")
  }

  test("convertBasicExpressions with Float literal") {
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal(1.2345F), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "1.2345")
  }

  test("convertBasicExpressions with Double literal") {
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal(3e5D), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "300000.0")
  }

  test("convertBasicExpressions with Byte literal") {
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(Literal(20.toByte), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "20")
  }

  test("convertBasicExpressions with non basic expression") {
    val nonBasicExpression = IsNotNull.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertBasicExpressions(nonBasicExpression, fields)
    assert(bigQuerySQLStatement.isEmpty)
  }

  test("convertBooleanExpressions with In") {
    val inExpression = In.apply(schoolIdAttributeReference, List(Literal(100), Literal(200)))
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(inExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "SUBQUERY_2.SCHOOLID IN ( 100 , 200 )")
  }

  test("convertBooleanExpressions with IsNull") {
    val isNullExpression = IsNull.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(isNullExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID IS NULL )")
  }

  test("convertBooleanExpressions with IsNotNull") {
    val isNotNullExpression = IsNotNull.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(isNotNullExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID IS NOT NULL )")
  }

  test("convertBooleanExpressions with Not EqualTo") {
    val notEqualToExpression = Not.apply(EqualTo.apply(schoolIdAttributeReference, Literal(1234L)))
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(notEqualToExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID != 1234 )")
  }

  test("convertBooleanExpressions with Not GreaterThanOrEqual") {
    val notGreaterThanOrEqualToExpression = Not.apply(GreaterThanOrEqual.apply(schoolIdAttributeReference, Literal(1234L)))
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(notGreaterThanOrEqualToExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID < 1234 )")
  }

  test("convertBooleanExpressions with Not LessThanOrEqual") {
    val notLessThanOrEqualToExpression = Not.apply(LessThanOrEqual.apply(schoolIdAttributeReference, Literal(1234L)))
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(notLessThanOrEqualToExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID > 1234 )")
  }

  test("convertBooleanExpressions with Not GreaterThan") {
    val notGreaterThanExpression = Not.apply(GreaterThan.apply(schoolIdAttributeReference, Literal(1234L)))
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(notGreaterThanExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID <= 1234 )")
  }

  test("convertBooleanExpressions with Not LessThan") {
    val notLessThanExpression = Not.apply(LessThan.apply(schoolIdAttributeReference, Literal(1234L)))
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(notLessThanExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID >= 1234 )")
  }

  test("convertBooleanExpressions with Contains") {
    val containsExpression = Contains.apply(schoolIdAttributeReference, Literal("1234"))
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(containsExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CONTAINS_SUBSTR ( SUBQUERY_2.SCHOOLID , '1234' )")
  }

  test("convertBooleanExpressions with Ends With") {
    val endsWithExpression = EndsWith.apply(schoolIdAttributeReference, Literal("1234"))
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(endsWithExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "ENDS_WITH ( SUBQUERY_2.SCHOOLID , '1234' )")
  }

  test("convertBooleanExpressions with Starts With") {
    val startsWithExpression = StartsWith.apply(schoolIdAttributeReference, Literal("1234"))
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(startsWithExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "STARTS_WITH ( SUBQUERY_2.SCHOOLID , '1234' )")
  }

  test("convertBooleanExpressions with non Boolean expression") {
    val bigQuerySQLStatement = expressionConverter.convertBooleanExpressions(Literal(100L), fields)
    assert(bigQuerySQLStatement.isEmpty)
  }

  test("convertStringExpressions with Ascii") {
    val asciiExpression = Ascii.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(asciiExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "ASCII ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertStringExpressions with Concat") {
    val concatExpression = Concat.apply(List(schoolIdAttributeReference, Literal("**")))
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(concatExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CONCAT ( SUBQUERY_2.SCHOOLID , '**' )")
  }

  test("convertStringExpressions with Length") {
    val lengthExpression = Length.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(lengthExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "LENGTH ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertStringExpressions with Lower") {
    val lowerExpression = Lower.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(lowerExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "LOWER ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertStringExpressions with StringLPad") {
    val stringLPadExpression = StringLPad.apply(schoolIdAttributeReference, Literal(10), Literal("*"))
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(stringLPadExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "LPAD ( SUBQUERY_2.SCHOOLID , 10 , '*' )")
  }

  test("convertStringExpressions with StringRPad") {
    val stringRPadExpression = StringRPad.apply(schoolIdAttributeReference, Literal(10), Literal("*"))
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(stringRPadExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "RPAD ( SUBQUERY_2.SCHOOLID , 10 , '*' )")
  }

  test("convertStringExpressions with StringTranslate") {
    val stringTranslateExpression = StringTranslate.apply(schoolIdAttributeReference, Literal("*"), Literal("**"))
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(stringTranslateExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "TRANSLATE ( SUBQUERY_2.SCHOOLID , '*' , '**' )")
  }

  test("convertStringExpressions with StringTrim") {
    val stringTrimExpression = StringTrim.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(stringTrimExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "TRIM ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertStringExpressions with StringTrimLeft") {
    val stringTrimLeftExpression = StringTrimLeft.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(stringTrimLeftExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "LTRIM ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertStringExpressions with StringTrimRight") {
    val stringTrimRightExpression = StringTrimRight.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(stringTrimRightExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "RTRIM ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertStringExpressions with Upper") {
    val upperExpression = Upper.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(upperExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "UPPER ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertStringExpressions with StringInstr") {
    val stringInstrExpression = StringInstr.apply(schoolIdAttributeReference, Literal("1234"))
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(stringInstrExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "INSTR ( SUBQUERY_2.SCHOOLID , '1234' )")
  }

  test("convertStringExpressions with InitCap") {
    val initCapExpression = InitCap.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(initCapExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "INITCAP ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertStringExpressions with Base64") {
    val base64Expression = Base64.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(base64Expression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "TO_BASE64 ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertStringExpressions with UnBase64") {
    val unBase64Expression = UnBase64.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(unBase64Expression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "FROM_BASE64 ( SUBQUERY_2.SCHOOLID )")
  }

  test("convertStringExpressions with Substring") {
    val substrExpression = Substring.apply(schoolIdAttributeReference, Literal(2), Literal(3))
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(substrExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "SUBSTRING ( SUBQUERY_2.SCHOOLID , 2 , 3 )")
  }

  test("convertStringExpressions with SoundEx") {
    val soundexExpression = SoundEx.apply(schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(soundexExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "UPPER ( SOUNDEX ( SUBQUERY_2.SCHOOLID ) )")
  }

  test("convertStringExpressions with RegExpExtract") {
    val regExpExtractExpression = RegExpExtract.apply(schoolIdAttributeReference, Literal("[0-9]"), Literal(1))
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(regExpExtractExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "REGEXP_EXTRACT ( SUBQUERY_2.SCHOOLID , r'[0-9]' , 1 )")
  }

  test("convertStringExpressions with RegExpReplace") {
    val regExpReplaceExpression = RegExpReplace.apply(schoolIdAttributeReference, Literal("[0-9]"), Literal("replace"))
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(regExpReplaceExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "REGEXP_REPLACE ( SUBQUERY_2.SCHOOLID , r'[0-9]' , 'replace' )")
  }

  test("convertStringExpressions with FormatString") {
    val formatStringExpression = FormatString.apply(Literal("*%s*"), schoolIdAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(formatStringExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "FORMAT ( '*%s*' , SUBQUERY_2.SCHOOLID )")
  }

  test("convertStringExpressions with FormatNumber") {
    val formatNumberExpression = FormatNumber.apply(Literal(12.3456), Literal(2))
    val bigQuerySQLStatement = expressionConverter.convertStringExpressions(formatNumberExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "FORMAT ( 12.3456 , 2 )")
  }

  test("convertMathematicalExpressions with Abs") {
    val absExpression = Abs.apply(Literal(-12))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(absExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "ABS ( -12 )")
  }

  test("convertMathematicalExpressions with Acos") {
    val aCosExpression = Acos.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(aCosExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "ACOS ( 90 )")
  }

  test("convertMathematicalExpressions with Asin") {
    val aSinExpression = Asin.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(aSinExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "ASIN ( 90 )")
  }

  test("convertMathematicalExpressions with Atan") {
    val aTanExpression = Atan.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(aTanExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "ATAN ( 90 )")
  }

  test("convertMathematicalExpressions with Cos") {
    val cosExpression = Cos.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(cosExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COS ( 90 )")
  }

  test("convertMathematicalExpressions with Cosh") {
    val coshExpression = Cosh.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(coshExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COSH ( 90 )")
  }

  test("convertMathematicalExpressions with Exp") {
    val expExpression = Exp.apply(Literal(2))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(expExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "EXP ( 2 )")
  }

  test("convertMathematicalExpressions with Floor") {
    val floorExpression = Floor.apply(Literal(2.5))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(floorExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "FLOOR ( 2.5 )")
  }

  test("convertMathematicalExpressions with Greatest") {
    val greatestExpression = Greatest.apply(List(Literal(1), Literal(3), Literal(2)))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(greatestExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "GREATEST ( 1 , 3 , 2 )")
  }

  test("convertMathematicalExpressions with Least") {
    val leastExpression = Least.apply(List(Literal(1), Literal(3), Literal(2)))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(leastExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "LEAST ( 1 , 3 , 2 )")
  }

  test("convertMathematicalExpressions with Logarithm") {
    val logExpression = Logarithm.apply(Literal(2), Literal(10))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(logExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "LOG ( 10 , 2 )")
  }

  test("convertMathematicalExpressions with Log10") {
    val logExpression = Log10.apply(Literal(10))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(logExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "LOG10 ( 10 )")
  }

  test("convertMathematicalExpressions with Pow") {
    val powExpression = Pow.apply(Literal(10), Literal(2))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(powExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "POWER ( 10 , 2 )")
  }

  test("convertMathematicalExpressions with Round") {
    val roundExpression = Round.apply(Literal(10.234), Literal(2))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(roundExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "ROUND ( 10.234 , 2 )")
  }

  test("convertMathematicalExpressions with Sin") {
    val sinExpression = Sin.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(sinExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "SIN ( 90 )")
  }

  test("convertMathematicalExpressions with Sinh") {
    val sinhExpression = Sinh.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(sinhExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "SINH ( 90 )")
  }

  test("convertMathematicalExpressions with Sqrt") {
    val sqrtExpression = Sqrt.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(sqrtExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "SQRT ( 90 )")
  }

  test("convertMathematicalExpressions with Tan") {
    val tanExpression = Tan.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(tanExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "TAN ( 90 )")
  }

  test("convertMathematicalExpressions with Tanh") {
    val tanhExpression = Tanh.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(tanhExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "TANH ( 90 )")
  }

  test("convertMathematicalExpressions with IsNaN") {
    val isNanExpression = IsNaN.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(isNanExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "IS_NAN ( 90 )")
  }

  test("convertMathematicalExpressions with Signum") {
    val signumExpression = Signum.apply(Literal(90))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(signumExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "SIGN ( 90 )")
  }

  test("convertMathematicalExpressions with Rand") {
    val randExpression = Rand.apply(Literal(2))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(randExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "RAND ()")
  }

  test("convertMathematicalExpressions with Pi") {
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(Pi(), fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "bqutil.fn.pi()")
  }

  test("convertMathematicalExpressions with PromotePrecision") {
    val promotePrecisionExpression = PromotePrecision(Cast(Literal.apply(233.00), DecimalType.apply(38, 6)))
    val bigQuerySQLStatement = expressionConverter.convertMathematicalExpressions(promotePrecisionExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( 233.0 AS BIGDECIMAL )")
  }

  test("convertMiscellaneousExpressions with Alias") {
    val aliasExpression = Alias.apply(schoolIdAttributeReference, "SCHOOL_ID_ALIAS")(ExprId.apply(1))
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(aliasExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID ) AS SCHOOL_ID_ALIAS")
  }

  test("convertMiscellaneousExpressions with Ascending sort") {
    val sortExpression = SortOrder.apply(schoolIdAttributeReference, Ascending)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(sortExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID ) ASC")
  }

  test("convertMiscellaneousExpressions with Descending sort") {
    val sortExpression = SortOrder.apply(schoolIdAttributeReference, Descending)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(sortExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SUBQUERY_2.SCHOOLID ) DESC")
  }

  test("convertMiscellaneousExpressions with unsupported casts") {
    assertThrows[BigQueryPushdownUnsupportedException] {
      expressionConverter.convertMiscellaneousExpressions(Cast.apply(AttributeReference.apply("Date", DateType)(ExprId.apply(2)), IntegerType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      expressionConverter.convertMiscellaneousExpressions(Cast.apply(AttributeReference.apply("Date", DateType)(ExprId.apply(2)), LongType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      expressionConverter.convertMiscellaneousExpressions(Cast.apply(AttributeReference.apply("Date", DateType)(ExprId.apply(2)), FloatType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      expressionConverter.convertMiscellaneousExpressions(Cast.apply(AttributeReference.apply("Date", DateType)(ExprId.apply(2)), DoubleType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      expressionConverter.convertMiscellaneousExpressions(Cast.apply(AttributeReference.apply("Date", DateType)(ExprId.apply(2)), DecimalType(10, 0)), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      expressionConverter.convertMiscellaneousExpressions(Cast.apply(AttributeReference.apply("Timestamp", TimestampType)(ExprId.apply(2)), IntegerType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      expressionConverter.convertMiscellaneousExpressions(Cast.apply(AttributeReference.apply("Timestamp", TimestampType)(ExprId.apply(2)), LongType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      expressionConverter.convertMiscellaneousExpressions(Cast.apply(AttributeReference.apply("Timestamp", TimestampType)(ExprId.apply(2)), FloatType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      expressionConverter.convertMiscellaneousExpressions(Cast.apply(AttributeReference.apply("Timestamp", TimestampType)(ExprId.apply(2)), DoubleType), fields)
    }

    assertThrows[BigQueryPushdownUnsupportedException] {
      expressionConverter.convertMiscellaneousExpressions(Cast.apply(AttributeReference.apply("Timestamp", TimestampType)(ExprId.apply(2)), DecimalType(10, 0)), fields)
    }
  }

  test("convertMiscellaneousExpressions with Cast from Integer to String") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", IntegerType)(ExprId.apply(2)), StringType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS STRING )")
  }

  test("convertMiscellaneousExpressions with Cast from Integer to Boolean") {
    val castExpression = Cast.apply(AttributeReference.apply("is_student", IntegerType)(ExprId.apply(2)), BooleanType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( IS_STUDENT AS BOOL )")
  }

  test("convertMiscellaneousExpressions with Cast from Integer to Short") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", IntegerType)(ExprId.apply(2)), ShortType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS INT64 )")
  }

  test("convertMiscellaneousExpressions with Cast from Integer to Long") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", IntegerType)(ExprId.apply(2)), LongType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS INT64 )")
  }

  test("convertMiscellaneousExpressions with Cast from Long to Integer") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", LongType)(ExprId.apply(2)), IntegerType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS INT64 )")
  }

  test("convertMiscellaneousExpressions with Cast from Integer to Float") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", IntegerType)(ExprId.apply(2)), FloatType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS FLOAT64 )")
  }

  test("convertMiscellaneousExpressions with Cast from Integer to Double") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", IntegerType)(ExprId.apply(2)), DoubleType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS FLOAT64 )")
  }

  test("convertMiscellaneousExpressions with Cast from Integer to Bytes") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", IntegerType)(ExprId.apply(2)), ByteType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS NUMERIC )")
  }

  test("convertMiscellaneousExpressions with Cast from Long to Bytes") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", LongType)(ExprId.apply(2)), ByteType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS NUMERIC )")
  }

  test("convertMiscellaneousExpressions with Cast from Float to Bytes") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", FloatType)(ExprId.apply(2)), ByteType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS NUMERIC )")
  }

  test("convertMiscellaneousExpressions with Cast from Double to Bytes") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", DoubleType)(ExprId.apply(2)), ByteType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS NUMERIC )")
  }

  test("convertMiscellaneousExpressions with Cast from Decimal to Bytes") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", DecimalType(10, 5))(ExprId.apply(2)), ByteType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS NUMERIC )")
  }

  test("convertMiscellaneousExpressions with Cast from String to Date") {
    val castExpression = Cast.apply(AttributeReference.apply("attendance_date", StringType)(ExprId.apply(2)), DateType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( ATTENDANCE_DATE AS DATE )")
  }

  test("convertMiscellaneousExpressions with Cast from String to Timestamp") {
    val castExpression = Cast.apply(AttributeReference.apply("last_modified", StringType)(ExprId.apply(2)), TimestampType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( LAST_MODIFIED AS TIMESTAMP )")
  }

  test("convertMiscellaneousExpressions with Cast from String to Bytes") {
    val castExpression = Cast.apply(AttributeReference.apply("SchoolID", StringType)(ExprId.apply(2)), ByteType)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( SCHOOLID AS BYTES )")
  }

  test("convertMiscellaneousExpressions with Cast from String to BigDecimal/BigNumeric") {
    val castExpression = Cast.apply(AttributeReference.apply("Transaction", StringType)(ExprId.apply(2)), DecimalType(10, 5))
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(castExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CAST ( TRANSACTION AS BIGDECIMAL )")
  }

  test("convertMiscellaneousExpressions with ShiftLeft") {
    val shiftLeftExpression = ShiftLeft.apply(Literal.apply(4), Literal.apply(2))
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(shiftLeftExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( 4 << 2 )")
  }

  test("convertMiscellaneousExpressions with ShiftRight") {
    val shiftRightExpression = ShiftRight.apply(Literal.apply(4), Literal.apply(2))
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(shiftRightExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( 4 >> 2 )")
  }

  test("convertMiscellaneousExpressions with CaseWhen") {
    val caseWhenExpression = CaseWhen.apply(List.apply(Tuple2.apply(Literal.apply("COND1"), Literal.apply("EVAL1")), Tuple2.apply(Literal.apply("COND2"), Literal.apply("EVAL2"))), Literal.apply("ELSE_EVAL"))
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(caseWhenExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CASE WHEN 'COND1' THEN 'EVAL1' WHEN 'COND2' THEN 'EVAL2' ELSE 'ELSE_EVAL' END")
  }

  test("convertMiscellaneousExpressions with CaseWhenWithWithoutElse") {
    val caseWhenExpression = CaseWhen.apply(List.apply(Tuple2.apply(Literal.apply("COND1"), Literal.apply("EVAL1")), Tuple2.apply(Literal.apply("COND2"), Literal.apply("EVAL2"))))
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(caseWhenExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "CASE WHEN 'COND1' THEN 'EVAL1' WHEN 'COND2' THEN 'EVAL2' END")
  }

  test("convertMiscellaneousExpressions with If") {
    val ifExpression = If.apply(Literal.apply("COND1"), Literal.apply("TRUE_VAL"), Literal.apply("FALSE_VAL"))
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(ifExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "IF ( 'COND1' , 'TRUE_VAL' , 'FALSE_VAL' )")
  }

  test("convertMiscellaneousExpressions with InSet") {
    val inSetExpression = InSet.apply(Literal.apply("COND1"), Set.apply(Literal.apply("C1"), Literal.apply("C2"), Literal.apply("C3")))
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(inSetExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "'COND1' IN ( 'C1' , 'C2' , 'C3' )")
  }

  test("convertMiscellaneousExpressions with UnscaledValue") {
    val inSetExpression = UnscaledValue.apply(Literal.apply(Decimal(1234.34)))
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(inSetExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( 1234.34 * POW( 10, 2 ) )")
  }

  test("convertMiscExpression with Coalesce") {
    val coalesceExpression = Coalesce.apply(List.apply(Literal.apply(null), Literal.apply(null), Literal.apply("COND2")))
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(coalesceExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "COALESCE ( NULL , NULL , 'COND2' )")
  }

  test("convertDateExpressions with DateAdd") {
    val dateAddExpression = DateAdd.apply(schoolStartDateAttributeReference, Literal.apply("1"))
    val bigQuerySQLStatement = expressionConverter.convertDateExpressions(dateAddExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "DATE_ADD ( STARTDATE , INTERVAL '1' DAY )")
  }

  test("convertDateExpressions with DateSub") {
    val dateSubExpression = DateSub.apply(schoolStartDateAttributeReference, Literal.apply("1"))
    val bigQuerySQLStatement = expressionConverter.convertDateExpressions(dateSubExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "DATE_SUB ( STARTDATE , INTERVAL '1' DAY )")
  }

  test("convertDateExpressions with Month") {
    val monthExpression = Month.apply(schoolStartDateAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertDateExpressions(monthExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "EXTRACT ( MONTH  FROM STARTDATE )")
  }

  test("convertDateExpressions with Quarter") {
    val quarterExpression = Quarter.apply(schoolStartDateAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertDateExpressions(quarterExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "EXTRACT ( QUARTER  FROM STARTDATE )")
  }

  test("convertDateExpressions with Year") {
    val yearExpression = Year.apply(schoolStartDateAttributeReference)
    val bigQuerySQLStatement = expressionConverter.convertDateExpressions(yearExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "EXTRACT ( YEAR  FROM STARTDATE )")
  }

  test("convertDateExpressions with DATE_TRUNC") {
    val yearExpression = TruncDate.apply(Literal.apply("2016-07-30"), Literal.apply("YEAR"))
    val bigQuerySQLStatement = expressionConverter.convertDateExpressions(yearExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "DATE_TRUNC ( '2016-07-30' , YEAR )")
  }

  test("convertWindowExpressions with RANK") {
    val rankExpression = Rank.apply(List.apply(Literal.apply(1)))
    val bigQuerySQLStatement = expressionConverter.convertWindowExpressions(rankExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "RANK ()")
  }

  test("convertWindowExpressions with DenseRank") {
    val denseRankExpression = DenseRank.apply(List.apply(Literal.apply(1)))
    val bigQuerySQLStatement = expressionConverter.convertWindowExpressions(denseRankExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "DENSE_RANK ()")
  }

  test("convertWindowExpressions with PercentRank") {
    val percentRankExpression = PercentRank.apply(List.apply(Literal.apply(1)))
    val bigQuerySQLStatement = expressionConverter.convertWindowExpressions(percentRankExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "PERCENT_RANK ()")
  }

  test("convertWindowExpressions with RowNumber") {
    val rowNumberExpression = RowNumber.apply()
    val bigQuerySQLStatement = expressionConverter.convertWindowExpressions(rowNumberExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "ROW_NUMBER ()")
  }
}
