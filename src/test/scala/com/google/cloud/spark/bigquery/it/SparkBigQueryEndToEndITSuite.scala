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
import java.util.TimeZone

import com.google.cloud.spark.bigquery.TestUtils
import com.google.cloud.spark.bigquery.it.TestConstants._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.concurrent.TimeLimits
import org.scalatest.time.SpanSugar._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SparkBigQueryEndToEndITSuite extends FunSuite
    with BeforeAndAfterAll with Matchers with TimeLimits {
  private val SHAKESPEARE_TABLE = "bigquery-public-data:samples.shakespeare"
  private val SHAKESPEARE_TABLE_NUM_ROWS = 164656L
  private val SHAKESPEARE_TABLE_SCHEMA = StructType(Seq(
    StructField("word", StringType, nullable = false),
    StructField("word_count", LongType, nullable = false),
    StructField("corpus", StringType, nullable = false),
    StructField("corpus_date", LongType, nullable = false)))
  private val LARGE_TABLE = "bigquery-public-data:samples.natality"
  private val LARGE_TABLE_FIELD = "is_male"
  private val LARGE_TABLE_NUM_ROWS = 137826763L
  private val NON_EXISTENT_TABLE = "non-existent:non-existent.non-existent"

  private var spark: SparkSession = _
  private var knownTable: DataFrame = _

  override def beforeAll: Unit = {
    spark = TestUtils.getOrCreateSparkSession()
    knownTable = spark.read.format("bigquery")
        .option("table", KNOWN_TABLE)
        .load()
  }

  override def afterAll: Unit = {
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
    import com.google.cloud.spark.bigquery._
    val expectedRow = spark.range(1).select(
      lit(42L),
      lit(null),
      lit(true),
      lit("string"),
      to_date(lit("2018-11-16")),
      from_utc_timestamp(lit("2018-11-16 22:16:37.713"), TimeZone.getDefault.getID),
      lit("2018-11-16T22:16:37.713004"),
      lit(80197713004L),
      lit(4.2),
      lit("bytes").cast("BINARY"),
      array(lit(1), lit(2), lit(3)),
      struct(lit(1), lit(2), lit(3)),
      array(struct(lit(1)))
    ).head.toSeq
    // Spark SQL has microsecond precision, but I can't figure out how to set it in a literal
    expectedRow(5).asInstanceOf[Timestamp].setNanos(713004000)
    val row = spark.read.bigquery(KNOWN_TABLE).head.toSeq
    row should contain theSameElementsInOrderAs expectedRow
  }


  test("known size in bytes") {
    val actualTableSize = knownTable.queryExecution.analyzed.stats.sizeInBytes
    assert(actualTableSize == KNOWN_TABLE_SIZE)
  }

  test("known schema") {
    assert(knownTable.schema == KNOWN_TABLE_SCHEMA)
  }

  test("user defined schema") {
    // TODO(pmkc): consider a schema that wouldn't cause cast errors if read.
    import com.google.cloud.spark.bigquery._
    val expectedSchema = StructType(Seq(StructField("whatever", ByteType)))
    val table = spark.read.schema(expectedSchema).bigquery(KNOWN_TABLE)
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

  test("bounded partition skew") {
    import com.google.cloud.spark.bigquery._
    // Usually finishes in 10, but there is a long tail.
    // TODO(pmkc): lower limit and add retries?
    failAfter(30 seconds) {
      // Select first partition
      val df = spark.read
          .option("parallelism", 999)
          .option("skewLimit", 1.0)
          .bigquery(LARGE_TABLE)
          .select(LARGE_TABLE_FIELD) // minimize payload
      val sizeOfFirstPartition = df.rdd.mapPartitionsWithIndex {
        case (0, it) => it
        case _ => Iterator.empty
      }.count
      // skewLimit is a soft limit and BigQuery can send an arbitrary amount of additional rows.
      // In this case it is usually 2X (booleans can be read very fast). In some rare cases
      // this can grow by a lot thus we assert that we see less than 10X desired.
      assert(sizeOfFirstPartition < (LARGE_TABLE_NUM_ROWS * 10.0 / df.rdd.getNumPartitions))
    }
  }
}
