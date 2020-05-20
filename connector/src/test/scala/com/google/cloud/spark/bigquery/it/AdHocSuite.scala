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

import com.google.cloud.bigquery.{BigQueryOptions, QueryJobConfiguration}
import com.google.cloud.spark.bigquery.it.TestConstants._
import com.google.cloud.spark.bigquery.{SparkBigQueryOptions, TestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.concurrent.TimeLimits
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.time.SpanSugar._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}

class AdHocSuite extends FunSuite
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
    spark = TestUtils.getOrCreateSparkSession()
  }

  test("test count") {
        val df = spark.read.format("com.google.cloud.spark.bigquery.BigQueryDataSourceV2")
          .option("table", "davidrab.alltypes")
          .load()

    spark.read.format("bigquery")
      .option("table", "davidrab.animals")
      .option("filter", "name like 'T%'")
      .option("readDataFormat", "ARROW")
      .load()
      .show()
    // scalastyle:off println
    // println(df.count)
    // scalastyle:on println

    // df.select("string_f", "record_f").show()
    // val dff = df.where("repository.url = 'https://github.com/apache/mahout'")
    // val dff = df.select("url, repository.url")

//    df.write.format("bigquery")
//       .option("temporaryGcsBucket", "davidrab-sandbox")
//        .option("table", "davidrab.dff")
//        .partitionBy("age")
//        .save()




//    val df = spark.read.format("bigquery")
//      .option("table", "davidrab.aview")
//      .option("viewsEnabled", "true")
//      .load()
//
//
//    println("df")
//    val dff = df.where("age = 1")
//    val data = dff.collect()
//    println("dff")
//    val dfz = df.where("age = 1000")
//    val dataz = dfz.collect()
//    println("dfz")
//    data.isEmpty shouldEqual false

//    df.write.format("bigquery")
//      .option("table", s"davidrab.animals_json_${System.nanoTime}")
//      .option("temporaryGcsBucket", "davidrab-sandbox")
//      .option("allowFieldAddition", "true")
//      .save()
  }

}
