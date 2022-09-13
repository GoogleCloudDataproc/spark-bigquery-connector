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

import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{SUBQUERY_0_ALIAS, TABLE_NAME, bigQueryRDDFactoryMock, expressionConverter, expressionFactory, schoolIdAttributeReference, schoolNameAttributeReference}
import org.scalatest.funsuite.AnyFunSuite

// Testing only the suffixStatement here since it is the only variable that is
// different from the other queries.
class AggregateQuerySuite extends AnyFunSuite{
  private val sourceQuery = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)

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
    val aggregateQuery = AggregateQuery(expressionConverter, expressionFactory, projectionColumns = Seq(), groups = Seq(), sourceQuery, SUBQUERY_0_ALIAS)
    val returnedQuery = aggregateQuery.find({ case q: SourceQuery => q })
    assert(returnedQuery.isDefined)
    assert(returnedQuery.get == sourceQuery)
  }
}
