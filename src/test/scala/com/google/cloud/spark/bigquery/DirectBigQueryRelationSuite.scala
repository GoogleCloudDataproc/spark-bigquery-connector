/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery

import com.google.cloud.bigquery._
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.mockito.Mockito._
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}


class DirectBigQueryRelationSuite
    extends FunSuite with BeforeAndAfter with MockitoSugar with Matchers {

  private val PROJECT_ID = "test_project"
  private val ID = TableId.of("test_project", "test_dataset", "test_table")
  private val TABLE_NAME = "test_project:test_dataset.test_table"
  @Mock
  private var sqlCtx: SQLContext = _

  private val TABLE = TableInfo.of(
    ID,
    StandardTableDefinition.newBuilder()
        .setSchema(Schema.of(
          Field.of("foo", LegacySQLTypeName.STRING),
          Field.of("bar", LegacySQLTypeName.INTEGER))
        )
        .setNumBytes(42L)
        .build())
  private var bigQueryRelation: DirectBigQueryRelation = _

  before {
    MockitoAnnotations.initMocks(this)
    bigQueryRelation = new DirectBigQueryRelation(
      SparkBigQueryOptions(ID, PROJECT_ID), TABLE)(sqlCtx)
  }

  after {
    validateMockitoUsage()
  }

  test("size in bytes") {
    assert(42 == bigQueryRelation.sizeInBytes)
  }

  test("schema") {
    val expectedSchema = StructType(Seq(
      StructField("foo", StringType), StructField("bar", LongType)))
    val schema = bigQueryRelation.schema
    assert(expectedSchema == schema)
  }

  // We don't have to be this permissive in user schemas, but we should at least allow
  // Long -> Int type changes
  test("user defined schema") {
    val expectedSchema = StructType(Seq(StructField("baz", ShortType)))
    bigQueryRelation = new DirectBigQueryRelation(
      SparkBigQueryOptions(ID, PROJECT_ID, schema = Some(expectedSchema)), TABLE)(sqlCtx)
    val schema = bigQueryRelation.schema
    assert(expectedSchema == schema)
  }

  test("valid filters") {
    val validFilters = Seq(
      GreaterThan("foo", "aardvark"),
      EqualTo("foo", "manatee"),
      LessThan("foo", "zebra"),
      LessThanOrEqual("bar", 1),
      GreaterThanOrEqual("bar", 2)
    )
    validFilters.foreach { f =>
      assert(bigQueryRelation.unhandledFilters(Array(f)).isEmpty)
    }
  }

  test("only first filter is valid") {
    val valid1 = EqualTo("foo", "bar")
    val valid2 = EqualTo("bar", 1)
    val unhandled = bigQueryRelation.unhandledFilters(Array(valid1, valid2))
    unhandled should (have length 1 and contain(valid2))
  }

  test("invalid filters") {
    val valid1 = EqualTo("foo", "bar")
    val valid2 = EqualTo("bar", 1)
    val invalidFilters: Array[Filter] = Array(
      IsNotNull("foo"),
      IsNull("foo"),
      Or(valid1, valid2),
      And(valid1, valid2)
    )
    val unhandled = bigQueryRelation.unhandledFilters(invalidFilters)
    unhandled should contain allElementsOf invalidFilters
  }
}

