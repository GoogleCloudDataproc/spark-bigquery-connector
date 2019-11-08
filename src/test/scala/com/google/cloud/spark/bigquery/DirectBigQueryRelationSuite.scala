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
        .setNumBytes(42L * 1000 * 1000 * 1000)  // 42GB
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
    assert(42L * 1000 * 1000 * 1000 == bigQueryRelation.sizeInBytes)
  }

  test("parallelism") {
    assert(105  == bigQueryRelation.getNumPartitionsRequested)
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
      EqualTo("foo", "manatee"),
      GreaterThan("foo", "aardvark"),
      GreaterThanOrEqual("bar", 2),
      LessThan("foo", "zebra"),
      LessThanOrEqual("bar", 1),
      In("foo", Array(1, 2, 3)),
      IsNull("foo"),
      IsNotNull("foo"),
      And(IsNull("foo"), IsNotNull("bar")),
      Or(IsNull("foo"), IsNotNull("foo")),
      Not(IsNull("foo")),
      StringStartsWith("foo", "abc"),
      StringEndsWith("foo", "def"),
      StringContains("foo", "abcdef")
    )
    validFilters.foreach { f =>
      assert(bigQueryRelation.unhandledFilters(Array(f)).isEmpty)
    }
  }

  test("multiple valid filters are handled") {
    val valid1 = EqualTo("foo", "bar")
    val valid2 = EqualTo("bar", 1)
    assert(bigQueryRelation.unhandledFilters(Array(valid1, valid2)).isEmpty)
  }

  test("invalid filters") {
    val valid1 = EqualTo("foo", "bar")
    val valid2 = EqualTo("bar", 1)
    val invalid1 = EqualNullSafe("foo", "bar")
    val invalid2 = And(EqualTo("foo", "bar"), Not(EqualNullSafe("bar", 1)))
    val unhandled = bigQueryRelation.unhandledFilters(Array(valid1, valid2, invalid1, invalid2))
    unhandled should contain allElementsOf Array(invalid1, invalid2)
  }

  test("old filter behaviour, with filter option") {
    val r = new DirectBigQueryRelation(
      SparkBigQueryOptions(
        ID, PROJECT_ID, combinePushedDownFilters = false, filter = Some("f>1")
      ), TABLE)(sqlCtx)
    checkFilters(r, "f>1", Array(GreaterThan("a", 2)), "f>1")
  }

  test("old filter behaviour, no filter option") {
    val r = new DirectBigQueryRelation(
      SparkBigQueryOptions(ID, PROJECT_ID, combinePushedDownFilters = false), TABLE)(sqlCtx)
    checkFilters(r, "", Array(GreaterThan("a", 2)), "a > 2")
  }

  test("new filter behaviour, with filter option") {
    val r = new DirectBigQueryRelation(
      SparkBigQueryOptions(ID, PROJECT_ID, filter = Some("f>1")), TABLE)(sqlCtx)
    checkFilters(r, "(f>1)", Array(GreaterThan("a", 2)), "(f>1) AND (a > 2)")
  }

  test("new filter behaviour, no filter option") {
    val r = new DirectBigQueryRelation(
      SparkBigQueryOptions(ID, PROJECT_ID), TABLE)(sqlCtx)
    checkFilters(r, "", Array(GreaterThan("a", 2)), "(a > 2)")
  }

  def checkFilters(
      r: DirectBigQueryRelation,
      resultWithoutFilters: String,
      filters: Array[Filter],
      resultWithFilters: String
      ): Unit = {
    val result1 = r.getCompiledFilter(Array())
    result1 shouldBe resultWithoutFilters
    val result2 = r.getCompiledFilter(filters)
    result2 shouldBe resultWithFilters
  }


}

