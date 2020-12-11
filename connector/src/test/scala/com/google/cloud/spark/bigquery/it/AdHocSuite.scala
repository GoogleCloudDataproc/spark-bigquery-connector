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

import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.spark.bigquery.TestUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.concurrent.TimeLimits
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Ignore, Matchers}

@Ignore
class AdHocITSuite extends FunSuite
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
  private var spark: SparkSession = _
  private var testDataset: String = _

  before {
    // have a fresh table for each test
    testTable = s"test_${System.nanoTime()}"
  }
  private var testTable: String = _
  private var allTypesTable: DataFrame = _

  override def beforeAll: Unit = {
    spark = TestUtils.getOrCreateSparkSession(getClass.getName)
    spark.conf.set("spark.sql.codegen.factoryMode", "NO_CODEGEN")

    // testDataset = s"spark_bigquery_it_${System.currentTimeMillis()}"
    // IntegrationTestUtils.createDataset(testDataset)
    // IntegrationTestUtils.runQuery(
    // TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE.format(s"$testDataset.$ALL_TYPES_TABLE_NAME"))
  }

  override def afterAll: Unit = {
    IntegrationTestUtils.deleteDatasetAndTables(testDataset)
    spark.stop()
  }

  def readAllTypesTable(dataSourceFormat: String): DataFrame =
    spark.read.format(dataSourceFormat)
      .option("dataset", testDataset)
      .option("table", ALL_TYPES_TABLE_NAME)
      .load()

  test("test ad hoc") {
    spark.conf.set("temporaryGcsBucket", "davidrab-sandbox")
//    val s = spark
//    import s.implicits._
//    val df = spark.createDataset[Foo](Seq(Foo("a", 1, true), Foo("b", 2, false))).toDF()
//    val table = s"davidrab.dsv2_write_${System.currentTimeMillis()}"
//    df.write.format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
//      .option("intermediateFormat", "parquet")
//      .save(table)
//
//    val outDf = spark.read.format("bigquery").load(table)
//    outDf.show()

    val df1 = spark.sql("select current_date() as d, current_timestamp() as ts")
//    val df1 = spark.read.format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
//      .load("davidrab.alltypes")
    val r1 = df1.head()
//    df1.write.format("avro").save("/tmp/test/alltypes.avro")
    df1.write.format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
      .mode("overwrite").save("davidrab.alltypes2")
    //
    //    val bqdf = spark.sql("select * from bigquery.`davidrab.demo`")
    //    bqdf.show()

    //
    //    val allTypesTable = readAllTypesTable()
    //    val expectedRow = spark.range(1).select(TestConstants.ALL_TYPES_TABLE_COLS: _*).head.toSeq
    //    val row = allTypesTable.head.toSeq
    //    row should contain theSameElementsInOrderAs expectedRow
  }

}

case class Foo(str: String, n: Int, b: Boolean)