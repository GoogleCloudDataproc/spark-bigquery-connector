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

import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{SUBQUERY_0_ALIAS, SUBQUERY_1_ALIAS, TABLE_NAME, bigQueryRDDFactoryMock, expressionConverter, expressionFactory, schoolIdAttributeReference, schoolNameAttributeReference}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Literal, SortOrder}
import org.scalatest.funsuite.AnyFunSuite

// Testing only the suffixStatement here since it is the only variable that is
// different from the other queries.
class SortLimitQuerySuite extends AnyFunSuite{
  private val sourceQuery = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)

  test("suffixStatement with only limit") {
    val sortLimitQuery = SortLimitQuery(expressionConverter, expressionFactory, Some(Literal(10)), Seq(), sourceQuery, SUBQUERY_1_ALIAS)
    assert(sortLimitQuery.suffixStatement.toString == "LIMIT 10")
  }

  test("suffixStatement with only orderBy") {
    val sortOrder = SortOrder.apply(schoolIdAttributeReference, Ascending)
    val sortLimitQuery = SortLimitQuery(expressionConverter, expressionFactory, None, Seq(sortOrder), sourceQuery, SUBQUERY_1_ALIAS)
    assert(sortLimitQuery.suffixStatement.toString == "ORDER BY ( SUBQUERY_0.SCHOOLID ) ASC")
  }

  test("suffixStatement with both orderBy and limit") {
    val sortOrder = SortOrder.apply(schoolIdAttributeReference, Ascending)
    val sortLimitQuery = SortLimitQuery(expressionConverter, expressionFactory, Some(Literal(10)), Seq(sortOrder), sourceQuery, SUBQUERY_1_ALIAS)
    assert(sortLimitQuery.suffixStatement.toString == "ORDER BY ( SUBQUERY_0.SCHOOLID ) ASC LIMIT 10")
  }

  test("find") {
    val sortOrder = SortOrder.apply(schoolIdAttributeReference, Ascending)
    val sortLimitQuery = SortLimitQuery(expressionConverter, expressionFactory, Some(Literal(10)), Seq(sortOrder), sourceQuery, SUBQUERY_1_ALIAS)
    val returnedQuery = sortLimitQuery.find({ case q: SourceQuery => q })
    assert(returnedQuery.isDefined)
    assert(returnedQuery.get == sourceQuery)
  }
}
