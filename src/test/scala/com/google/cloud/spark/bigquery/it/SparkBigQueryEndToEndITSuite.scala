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
package com.google.cloud.spark.bigquery.it

import java.sql.Timestamp

import com.google.cloud.spark.bigquery.TestUtils
import com.google.cloud.spark.bigquery.it.TestConstants._
import com.google.common.primitives.Bytes
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.concurrent.TimeLimits
import org.scalatest.time.SpanSugar._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SparkBigQueryEndToEndITSuite extends FunSuite
    with BeforeAndAfterAll with Matchers with TimeLimits {
  private val SHAKESPEARE_TABLE = "bigquery-public-data.samples.shakespeare"
  private val SHAKESPEARE_TABLE_NUM_ROWS = 164656L
  private val SHAKESPEARE_TABLE_SCHEMA = StructType(Seq(
    StructField("word", StringType, nullable = false),
    StructField("word_count", LongType, nullable = false),
    StructField("corpus", StringType, nullable = false),
    StructField("corpus_date", LongType, nullable = false)))
  private val LARGE_TABLE = "bigquery-public-data.samples.natality"
  private val LARGE_TABLE_FIELD = "is_male"
  private val LARGE_TABLE_NUM_ROWS = 137826763L
  private val NON_EXISTENT_TABLE = "non-existent.non-existent.non-existent"
  private val ALL_TYPES_TABLE_NAME = "all_types"

  private val log: Logger = Logger(getClass)

  private var spark: SparkSession = _
  private var testDataset: String = _
  private var allTypesTable: DataFrame = _

  override def beforeAll: Unit = {
    spark = TestUtils.getOrCreateSparkSession()
    testDataset = s"spark_bigquery_it_${System.currentTimeMillis()}"
    IntegrationTestUtils.createDataset(
      testDataset)
    IntegrationTestUtils.runQuery(
      TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE.format(s"$testDataset.$ALL_TYPES_TABLE_NAME"))
    allTypesTable = spark.read.format("bigquery")
        .option("dataset", testDataset)
        .option("table", ALL_TYPES_TABLE_NAME)
        .load()
  }

  override def afterAll: Unit = {
    IntegrationTestUtils.deleteDatasetAndTables(testDataset)
    spark.stop()
  }

  /** Generate a test that the given DataFrame is equal to a known BigQuery Table. */
  def testShakespeare(description: String)(df: => DataFrame): Unit = {
    test(description) {
      val youCannotImportVars = spark
      import youCannotImportVars.implicits._
      assert(SHAKESPEARE_TABLE_SCHEMA == df.schema)
      assert(SHAKESPEARE_TABLE_NUM_ROWS == df.count())
      val firstWords = df.select("word")
          .where("word >= 'a' AND word not like '%\\'%'")
          .distinct
          .as[String].sort("word").take(3)
      firstWords should contain theSameElementsInOrderAs Seq("a", "abaissiez", "abandon")
    }
  }

  testShakespeare("implicit read method") {
    import com.google.cloud.spark.bigquery._
    spark.read.bigquery(SHAKESPEARE_TABLE)
  }

  testShakespeare("explicit format") {
    spark.read.format("com.google.cloud.spark.bigquery")
        .option("table", SHAKESPEARE_TABLE)
        .load()
  }

  testShakespeare("short format") {
    spark.read.format("bigquery").option("table", SHAKESPEARE_TABLE).load()
  }

  test("out of order columns") {
    val row = spark.read.format("bigquery").option("table", SHAKESPEARE_TABLE).load()
        .select("word_count", "word").head
    assert(row(0).isInstanceOf[Long])
    assert(row(1).isInstanceOf[String])
  }


  test("number of partitions") {
    val df = spark.read.format("com.google.cloud.spark.bigquery")
        .option("table", LARGE_TABLE)
        .option("parallelism", "5")
        .load()
    assert(5 == df.rdd.getNumPartitions)
  }

  test("default number of partitions") {
    // This should be stable in our master local test environment.
    val expectedParallelism = spark.sparkContext.defaultParallelism
    val df = spark.read.format("com.google.cloud.spark.bigquery")
        .option("table", LARGE_TABLE)
        .load()
    assert(expectedParallelism == df.rdd.getNumPartitions)
  }

  test("read data types") {
    val expectedRow = spark.range(1).select(TestConstants.ALL_TYPES_TABLE_COLS: _*).head.toSeq
    val row = allTypesTable.head.toSeq
    row should contain theSameElementsInOrderAs expectedRow
  }


  test("known size in bytes") {
    val actualTableSize = allTypesTable.queryExecution.analyzed.stats.sizeInBytes
    assert(actualTableSize == ALL_TYPES_TABLE_SIZE)
  }

  test("known schema") {
    assert(allTypesTable.schema == ALL_TYPES_TABLE_SCHEMA)
  }

  test("user defined schema") {
    // TODO(pmkc): consider a schema that wouldn't cause cast errors if read.
    import com.google.cloud.spark.bigquery._
    val expectedSchema = StructType(Seq(StructField("whatever", ByteType)))
    val table = spark.read.schema(expectedSchema).bigquery(SHAKESPEARE_TABLE)
    assert(expectedSchema == table.schema)
  }

  test("non-existent schema") {
    import com.google.cloud.spark.bigquery._
    assertThrows[RuntimeException] {
      spark.read.bigquery(NON_EXISTENT_TABLE)
    }
  }

  test("head does not time out and OOM") {
    import com.google.cloud.spark.bigquery._
    failAfter(3 seconds) {
      spark.read.bigquery(LARGE_TABLE).select(LARGE_TABLE_FIELD).head
    }
  }

  test("balanced partitions") {
    import com.google.cloud.spark.bigquery._
    failAfter(60 seconds) {
      // Select first partition
      val df = spark.read
          .option("parallelism", 5)
          .bigquery(LARGE_TABLE)
          .select(LARGE_TABLE_FIELD) // minimize payload
      val sizeOfFirstPartition = df.rdd.mapPartitionsWithIndex {
        case (0, it) => it
        case _ => Iterator.empty
      }.count

      // Since we are only reading from a single stream, we can expect to get at least as many rows
      // in that stream as a perfectly uniform distribution would command. Note that the assertion
      // is on a range of rows because rows are assigned to streams on the server-side in
      // indivisible units of many rows.
      val numRowsLowerBound = LARGE_TABLE_NUM_ROWS / df.rdd.getNumPartitions
      assert(numRowsLowerBound <= sizeOfFirstPartition &&
          sizeOfFirstPartition < (numRowsLowerBound * 1.1).toInt)
    }
  }
}
