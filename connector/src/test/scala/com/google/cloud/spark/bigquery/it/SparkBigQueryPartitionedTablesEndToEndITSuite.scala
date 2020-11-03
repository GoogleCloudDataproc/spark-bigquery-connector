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

import com.google.cloud.bigquery._
import com.google.cloud.spark.bigquery.TestUtils
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import com.google.cloud.spark.bigquery.it.TestConstants._
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.scalatest.concurrent.TimeLimits
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.time.SpanSugar._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConverters._

class SparkBigQueryPartitionedTablesEndToEndITSuite extends FunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll
  with Matchers
  with TimeLimits
  with TableDrivenPropertyChecks {

  val filterData = Table(
    ("condition", "elements"),
    ("word_count == 4", Seq("'A", "'But", "'Faith")),
    ("word_count > 3", Seq("'", "''Tis", "'A")),
    ("word_count >= 2", Seq("'", "''Lo", "''O")),
    ("word_count < 3", Seq("''All", "''Among", "''And")),
    ("word_count <= 5", Seq("'", "''All", "''Among")),
    ("word_count in(8, 9)", Seq("'", "'Faith", "'Tis")),
    ("word_count is null", Seq()),
    ("word_count is not null", Seq("'", "''All", "''Among")),
    ("word_count == 4 and corpus == 'twelfthnight'", Seq("'Thou", "'em", "Art")),
    ("word_count == 4 or corpus > 'twelfthnight'", Seq("'", "''Tis", "''twas")),
    ("not word_count in(8, 9)", Seq("'", "''All", "''Among")),
    ("corpus like 'king%'", Seq("'", "'A", "'Affectionate")),
    ("corpus like '%kinghenryiv'", Seq("'", "'And", "'Anon")),
    ("corpus like '%king%'", Seq("'", "'A", "'Affectionate"))
  )
  val temporaryGcsBucket = "davidrab-sandbox"
  val bq = BigQueryOptions.getDefaultInstance.getService
  private var spark: SparkSession = _
  private var testDataset: String = _

  before {
    // have a fresh table for each test
    testTable = s"test_${System.nanoTime()}"
  }
  private var testTable: String = _

  override def beforeAll: Unit = {
    spark = TestUtils.getOrCreateSparkSession()
    testDataset = s"spark_bigquery_it_${System.currentTimeMillis()}"
    IntegrationTestUtils.createDataset(testDataset)
  }

  override def afterAll: Unit = {
    IntegrationTestUtils.deleteDatasetAndTables(testDataset)
    spark.stop()
  }



  test("hourly partition") {
    testPartition("HOUR")
  }

  test("daily partition") {
    testPartition("DAY")
  }

  test("monthly partition") {
    testPartition("MONTH")
  }

  test("yearly partition") {
    testPartition("YEAR")
  }

  def testPartition(partitionType: String): Unit = {
    val s = spark // cannot import from a var
    import s.implicits._
    val df = spark.createDataset(Seq(
      Data("a", java.sql.Timestamp.valueOf("2020-01-01 01:01:01")),
      Data("b", java.sql.Timestamp.valueOf("2020-01-02 02:02:02")),
      Data("c", java.sql.Timestamp.valueOf("2020-01-03 03:03:03"))
    )).toDF()

    val table = s"${testDataset}.${testTable}_${partitionType}"
    df.write.format("bigquery")
      .option("temporaryGcsBucket", temporaryGcsBucket)
      .option("partitionField", "d")
      .option("partitionType", partitionType)
      .option("partitionRequireFilter", "true")
      .option("table", table)
      .save()

    val readDF = spark.read.format("bigquery").load("table")
    assert(readDF.count == 3)
  }
 }

case class Data(str: String, t: java.sql.Timestamp)