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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId}
import org.apache.spark.sql.types.LongType
import org.scalatest.funsuite.AnyFunSuite

class SparkBigQueryPushdownUtilSuite extends AnyFunSuite {
  test("blockStatement with EmptyBigQuerySQLStatement") {
    val bigQuerySQLStatement = EmptyBigQuerySQLStatement.apply()
    val returnedStatement = SparkBigQueryPushdownUtil.blockStatement(bigQuerySQLStatement)
    assert("( )" == returnedStatement.toString)
  }

  test("blockStatement with BigQuerySQLStatement") {
    val bigQuerySQLStatement = ConstantString("Address") + "IS NOT NULL"
    val returnedStatement = SparkBigQueryPushdownUtil.blockStatement(bigQuerySQLStatement)
    assert("( Address IS NOT NULL )" == returnedStatement.toString)
  }

  test("blockStatement with alias") {
    val bigQuerySQLStatement = ConstantString("`bigquery-public-data.samples.shakespeare`").toStatement
    val returnedStatement = SparkBigQueryPushdownUtil.blockStatement(bigQuerySQLStatement, "bq_connector_query_alias")
    assert("( `bigquery-public-data.samples.shakespeare` ) AS BQ_CONNECTOR_QUERY_ALIAS" == returnedStatement.toString)
  }

  test("makeStatement") {
    val bigQuerySQLStatement1 = ConstantString("Name").toStatement
    val bigQuerySQLStatement2 = ConstantString("Address").toStatement
    val bigQuerySQLStatement3 = ConstantString("Email").toStatement
    val returnedStatement = SparkBigQueryPushdownUtil.makeStatement(List(bigQuerySQLStatement1, bigQuerySQLStatement2, bigQuerySQLStatement3), ",")
    assert("Name , Address , Email" == returnedStatement.toString)
  }

  test("addAttributeStatement with field match") {
    // Field match will occur on ExprId
    val attr = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
    val fields = List(AttributeReference.apply("SUBQUERY_2_COL_0", LongType)(ExprId.apply(1), List("SUBQUERY_2")),
      AttributeReference.apply("SUBQUERY_3_COL_1", LongType)(ExprId.apply(2), List("SUBQUERY_3")))
    val returnedStatement = SparkBigQueryPushdownUtil.addAttributeStatement(attr, fields)
    assert("SUBQUERY_2.SUBQUERY_2_COL_0" == returnedStatement.toString)
  }

  test("addAttributeStatement without field match") {
    // Field match will not occur since ExprId are different between attr and fields
    val attr = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
    val fields = List(AttributeReference.apply("SUBQUERY_2_COL_0", LongType)(ExprId.apply(2), List("SUBQUERY_2")),
      AttributeReference.apply("SUBQUERY_3_COL_1", LongType)(ExprId.apply(3), List("SUBQUERY_3")))
    val returnedStatement = SparkBigQueryPushdownUtil.addAttributeStatement(attr, fields)
    assert("SCHOOLID" == returnedStatement.toString)
  }

  test("renameColumns") {
    val namedExpr1 = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
    val namedExpr2 = Alias.apply(namedExpr1, "SID")(ExprId.apply(2))
    val returnedExpressions = SparkBigQueryPushdownUtil.renameColumns(List(namedExpr1, namedExpr2), "SUBQUERY_2")
    assert(2 == returnedExpressions.size)
    assert("SUBQUERY_2_COL_0" == returnedExpressions.head.name)
    assert(namedExpr1.exprId == returnedExpressions.head.exprId)
    assert("SUBQUERY_2_COL_1" == returnedExpressions(1).name)
    assert(namedExpr2.exprId == returnedExpressions(1).exprId)
  }
}
