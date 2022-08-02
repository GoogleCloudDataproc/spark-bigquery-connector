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

import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{SUBQUERY_0_ALIAS, SUBQUERY_1_ALIAS, SUBQUERY_2_ALIAS, bigQueryRDDFactoryMock, expressionConverter, expressionFactory, schoolIdAttributeReference}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, ExprId, GreaterThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.funsuite.AnyFunSuite

class JoinQuerySuite extends AnyFunSuite {
  // Two tables on which we want to join
  val SCHOOL_TABLE_NAME = "test_project:test_dataset.school"
  val STUDENT_TABLE_NAME = "test_project:test_dataset.student"

  // studentName and schoolName are two columns in the student table and school table respectively
  val studentNameAttributeReference: AttributeReference = AttributeReference.apply("StudentName", StringType)(ExprId.apply(2))
  val schoolNameAttributeReference: AttributeReference = AttributeReference.apply("SchoolName", StringType)(ExprId.apply(3))

  // Consider that we are joining on the column SchoolID in the school table with _SchoolID in the student table
  val _schoolIdAttributeReference: AttributeReference = AttributeReference.apply("_SchoolID", LongType)(ExprId.apply(4))
  private val joinExpression = EqualTo(schoolIdAttributeReference, _schoolIdAttributeReference)

  // Left query of the join
  private val leftSourceQuery = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, SCHOOL_TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)
  private val greaterThanFilterCondition = GreaterThanOrEqual.apply(schoolIdAttributeReference, Literal(50))
  private val filterQuery = FilterQuery(expressionConverter, expressionFactory, Seq(greaterThanFilterCondition), leftSourceQuery, SUBQUERY_1_ALIAS)

  // Right query of the join
  private val rightSourceQuery = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, STUDENT_TABLE_NAME, Seq(_schoolIdAttributeReference, studentNameAttributeReference), SUBQUERY_2_ALIAS)

  test("find") {
    val joinQuery = JoinQuery(expressionConverter, expressionFactory, filterQuery, rightSourceQuery, Option.apply(joinExpression), JoinType.apply("inner"), "SUBQUERY_3")
    val returnedQuery = joinQuery.find({ case q: SourceQuery => q })
    assert(returnedQuery.isDefined)
    assert(returnedQuery.get == leftSourceQuery)
  }

  test("suffixStatement") {
    val joinQuery = JoinQuery(expressionConverter, expressionFactory, filterQuery, rightSourceQuery, Option.apply(joinExpression), JoinType.apply("inner"), "SUBQUERY_3")
    assert(joinQuery.suffixStatement.toString == "ON ( SUBQUERY_1.SCHOOLID = SUBQUERY_2._SCHOOLID )")
  }

  // Even though these statements look like a scary wall of text we are basically
  // selecting schoolId, schoolName, _schoolId and studentName from the two tables
  test("getStatement with INNER JOIN") {
    val joinQuery = JoinQuery(expressionConverter, expressionFactory, leftSourceQuery, rightSourceQuery, Option.apply(joinExpression), JoinType.apply("inner"), "SUBQUERY_3")
    assert(joinQuery.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_3_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_3_COL_1 , " +
      "( SUBQUERY_2._SCHOOLID ) AS SUBQUERY_3_COL_2 , ( SUBQUERY_2.STUDENTNAME ) AS SUBQUERY_3_COL_3 FROM " +
      "( SELECT * FROM `test_project:test_dataset.school` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 INNER JOIN " +
      "( SELECT * FROM `test_project:test_dataset.student` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_2 ON ( SUBQUERY_0.SCHOOLID = SUBQUERY_2._SCHOOLID )")
  }

  test("getStatement with LEFT OUTER JOIN") {
    val joinQuery = JoinQuery(expressionConverter, expressionFactory, leftSourceQuery, rightSourceQuery, Option.apply(joinExpression), JoinType.apply("leftouter"), "SUBQUERY_3")
    assert(joinQuery.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_3_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_3_COL_1 , " +
      "( SUBQUERY_2._SCHOOLID ) AS SUBQUERY_3_COL_2 , ( SUBQUERY_2.STUDENTNAME ) AS SUBQUERY_3_COL_3 FROM " +
      "( SELECT * FROM `test_project:test_dataset.school` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 LEFT OUTER JOIN " +
      "( SELECT * FROM `test_project:test_dataset.student` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_2 ON ( SUBQUERY_0.SCHOOLID = SUBQUERY_2._SCHOOLID )")
  }

  test("getStatement with RIGHT OUTER JOIN") {
    val joinQuery = JoinQuery(expressionConverter, expressionFactory, leftSourceQuery, rightSourceQuery, Option.apply(joinExpression), JoinType.apply("rightouter"), "SUBQUERY_3")
    assert(joinQuery.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_3_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_3_COL_1 , " +
      "( SUBQUERY_2._SCHOOLID ) AS SUBQUERY_3_COL_2 , ( SUBQUERY_2.STUDENTNAME ) AS SUBQUERY_3_COL_3 FROM " +
      "( SELECT * FROM `test_project:test_dataset.school` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 RIGHT OUTER JOIN " +
      "( SELECT * FROM `test_project:test_dataset.student` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_2 ON ( SUBQUERY_0.SCHOOLID = SUBQUERY_2._SCHOOLID )")
  }

  test("getStatement with FULL OUTER JOIN") {
    val joinQuery = JoinQuery(expressionConverter, expressionFactory, leftSourceQuery, rightSourceQuery, Option.apply(joinExpression), JoinType.apply("fullouter"), "SUBQUERY_3")
    assert(joinQuery.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_3_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_3_COL_1 , " +
      "( SUBQUERY_2._SCHOOLID ) AS SUBQUERY_3_COL_2 , ( SUBQUERY_2.STUDENTNAME ) AS SUBQUERY_3_COL_3 FROM " +
      "( SELECT * FROM `test_project:test_dataset.school` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 FULL OUTER JOIN " +
      "( SELECT * FROM `test_project:test_dataset.student` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_2 ON ( SUBQUERY_0.SCHOOLID = SUBQUERY_2._SCHOOLID )")
  }

  test("getStatement with CROSS JOIN") {
    val joinQuery = JoinQuery(expressionConverter, expressionFactory, leftSourceQuery, rightSourceQuery, Option.apply(joinExpression), JoinType.apply("cross"), "SUBQUERY_3")
    assert(joinQuery.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_3_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_3_COL_1 , " +
      "( SUBQUERY_2._SCHOOLID ) AS SUBQUERY_3_COL_2 , ( SUBQUERY_2.STUDENTNAME ) AS SUBQUERY_3_COL_3 FROM " +
      "( SELECT * FROM `test_project:test_dataset.school` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 CROSS JOIN " +
      "( SELECT * FROM `test_project:test_dataset.student` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_2 ON ( SUBQUERY_0.SCHOOLID = SUBQUERY_2._SCHOOLID )")
  }

  test("getStatement with LEFT SEMI JOIN") {
    val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")
    val joinQuery = LeftSemiJoinQuery(expressionConverter, expressionFactory, leftSourceQuery, rightSourceQuery, Option.apply(joinExpression), isAntiJoin = false, alias)
    assert(joinQuery.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_0_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_0_COL_1 FROM " +
      "( SELECT * FROM `test_project:test_dataset.school` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 WHERE  EXISTS " +
      "( SELECT * FROM ( SELECT * FROM `test_project:test_dataset.student` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_2 WHERE " +
      "( SUBQUERY_0.SCHOOLID = SUBQUERY_2._SCHOOLID ) )")
  }

  test("getStatement with LEFT ANTI JOIN") {
    val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")
    val joinQuery = LeftSemiJoinQuery(expressionConverter, expressionFactory, leftSourceQuery, rightSourceQuery, Option.apply(joinExpression), isAntiJoin = true, alias)
    assert(joinQuery.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_0_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_0_COL_1 FROM " +
      "( SELECT * FROM `test_project:test_dataset.school` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 WHERE  NOT EXISTS " +
      "( SELECT * FROM ( SELECT * FROM `test_project:test_dataset.student` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_2 WHERE " +
      "( SUBQUERY_0.SCHOOLID = SUBQUERY_2._SCHOOLID ) )")
  }

  test("find for left semi join") {
    val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")
    val joinQuery = LeftSemiJoinQuery(expressionConverter, expressionFactory, leftSourceQuery, rightSourceQuery, Option.apply(joinExpression), isAntiJoin = true, alias)
    val returnedQuery = joinQuery.find({ case q: SourceQuery => q })
    assert(returnedQuery.isDefined)
    assert(returnedQuery.get == leftSourceQuery)
  }

  test("suffixStatement for left semi join") {
    val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")
    val joinQuery = LeftSemiJoinQuery(expressionConverter, expressionFactory, leftSourceQuery, rightSourceQuery, Option.apply(joinExpression), isAntiJoin = false, alias)
    assert(joinQuery.suffixStatement.toString == "WHERE  EXISTS " +
      "( SELECT * FROM ( SELECT * FROM `test_project:test_dataset.student` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_2 WHERE " +
      "( SUBQUERY_0.SCHOOLID = SUBQUERY_2._SCHOOLID ) )")
  }

  test("suffixStatement for left anti join") {
    val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")
    val joinQuery = LeftSemiJoinQuery(expressionConverter, expressionFactory, leftSourceQuery, rightSourceQuery, Option.apply(joinExpression), isAntiJoin = true, alias)
    assert(joinQuery.suffixStatement.toString == "WHERE  NOT EXISTS " +
      "( SELECT * FROM ( SELECT * FROM `test_project:test_dataset.student` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_2 WHERE " +
      "( SUBQUERY_0.SCHOOLID = SUBQUERY_2._SCHOOLID ) )")
  }
}
