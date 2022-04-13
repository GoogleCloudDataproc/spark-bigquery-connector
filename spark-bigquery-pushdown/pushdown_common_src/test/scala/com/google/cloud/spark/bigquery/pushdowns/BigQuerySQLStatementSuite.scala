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

import org.scalatest.funsuite.AnyFunSuite

class BigQuerySQLStatementSuite extends AnyFunSuite {
  test("toString") {
    val bigQuerySQLStatement = EmptyBigQuerySQLStatement.apply()
    val newBigQuerySQLStatement = bigQuerySQLStatement + ConstantString("LIMIT") + IntVariable(Option.apply(5))
    assert("LIMIT 5" == newBigQuerySQLStatement.toString)
  }

  test("+ with StatementElement") {
    val bigQuerySQLStatement = EmptyBigQuerySQLStatement.apply()
    val newBigQuerySQLStatement = bigQuerySQLStatement + ConstantString("LIMIT") + IntVariable(Option.apply(5))
    assert(2 == newBigQuerySQLStatement.list.size)
    assert("5" == newBigQuerySQLStatement.list.head.sql)
    assert("LIMIT" == newBigQuerySQLStatement.list(1).sql)
    assert("LIMIT 5" == newBigQuerySQLStatement.toString)
  }

  test("+ with BigQuerySQLStatement") {
    val bigQuerySQLStatement1 = ConstantString("SELECT") + "*"
    val bigQuerySQLStatement2 = ConstantString("FROM TABLE")

    val newBigQuerySQLStatement = bigQuerySQLStatement1 + bigQuerySQLStatement2
    assert(3 == newBigQuerySQLStatement.list.size)
    assert("FROM TABLE" == newBigQuerySQLStatement.list.head.sql)
    assert("*" == newBigQuerySQLStatement.list(1).sql)
    assert("SELECT" == newBigQuerySQLStatement.list(2).sql)
    assert("SELECT * FROM TABLE" == newBigQuerySQLStatement.toString)
  }

  test("+ with String") {
    val bigQuerySQLStatement = EmptyBigQuerySQLStatement.apply()
    val newBigQuerySQLStatement = bigQuerySQLStatement + "GROUP BY"
    assert(1 == newBigQuerySQLStatement.list.size)
    assert("GROUP BY" == newBigQuerySQLStatement.list.head.sql)
    assert("GROUP BY" == newBigQuerySQLStatement.toString)
  }
}
