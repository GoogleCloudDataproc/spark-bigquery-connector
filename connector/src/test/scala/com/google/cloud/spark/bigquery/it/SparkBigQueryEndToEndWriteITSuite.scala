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

import java.util.UUID

import com.google.cloud.bigquery._
import com.google.cloud.spark.bigquery.{SchemaConverters, TestUtils}
import com.google.common.base.Preconditions
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

class SparkBigQueryEndToEndWriteITSuite extends FunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll
  with Matchers
  with TimeLimits
  with TableDrivenPropertyChecks {

  val TemporaryGcsBucketEnvVariable = "TEMPORARY_GCS_BUCKET"

  val temporaryGcsBucket = Preconditions.checkNotNull(
    System.getenv(TemporaryGcsBucketEnvVariable),
    "Please set the %s env variable to point to a write enabled GCS bucket",
    TemporaryGcsBucketEnvVariable)
  val bq = BigQueryOptions.getDefaultInstance.getService
  private val LIBRARIES_PROJECTS_TABLE = "bigquery-public-data.libraries_io.projects"
  private val ALL_TYPES_TABLE_NAME = "all_types"
  private var spark: SparkSession = _
  private var testDataset: String = _

  private def metadata(key: String, value: String): Metadata = metadata(Map(key -> value))

  private def metadata(map: Map[String, String]): Metadata = {
    val metadata = new MetadataBuilder()
    for ((key, value) <- map) {
      metadata.putString(key, value)
    }
    metadata.build()
  }

  before {
    // have a fresh table for each test
    testTable = s"test_${System.nanoTime()}"
  }
  private var testTable: String = _

  override def beforeAll: Unit = {
    spark = TestUtils.getOrCreateSparkSession("SparkBigQueryEndToEndWriteITSuite")
    //    spark.conf.set("spark.sql.codegen.factoryMode", "NO_CODEGEN")
    //    System.setProperty("spark.testing", "true")
    testDataset = s"spark_bigquery_it_${System.currentTimeMillis()}"
    IntegrationTestUtils.createDataset(testDataset)
    IntegrationTestUtils.runQuery(
      TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE.format(s"$testDataset.$ALL_TYPES_TABLE_NAME"))
  }


  // Write tests. We have four save modes: Append, ErrorIfExists, Ignore and
  // Overwrite. For each there are two behaviours - the table exists or not.
  // See more at http://spark.apache.org/docs/2.3.2/api/java/org/apache/spark/sql/SaveMode.html

  override def afterAll: Unit = {
    IntegrationTestUtils.deleteDatasetAndTables(testDataset)
  }

  private def initialData = spark.createDataFrame(spark.sparkContext.parallelize(
    Seq(Person("Abc", Seq(Friend(10, Seq(Link("www.abc.com"))))),
      Person("Def", Seq(Friend(12, Seq(Link("www.def.com"))))))))

  private def additonalData = spark.createDataFrame(spark.sparkContext.parallelize(
    Seq(Person("Xyz", Seq(Friend(10, Seq(Link("www.xyz.com"))))),
      Person("Pqr", Seq(Friend(12, Seq(Link("www.pqr.com"))))))))

  // getNumRows returns BigInteger, and it messes up the matchers
  private def testTableNumberOfRows = bq.getTable(testDataset, testTable).getNumRows.intValue

  private def testPartitionedTableDefinition = bq.getTable(testDataset, testTable + "_partitioned")
    .getDefinition[StandardTableDefinition]()

  private def writeToBigQuery(
                               dataSource: String,
                               df: DataFrame,
                               mode: SaveMode,
                               format: String = "parquet") =
    df.write.format(dataSource)
      .mode(mode)
      .option("table", fullTableName)
      .option("temporaryGcsBucket", temporaryGcsBucket)
      .option("intermediateFormat", format)
      .save()

  private def initialDataValuesExist = numberOfRowsWith("Abc") == 1

  private def numberOfRowsWith(name: String) =
    bq.query(QueryJobConfiguration.of(s"select name from $fullTableName where name='$name'"))
      .getTotalRows

  private def fullTableName = s"$testDataset.$testTable"

  private def fullTableNamePartitioned = s"$testDataset.${testTable}_partitioned"

  private def additionalDataValuesExist = numberOfRowsWith("Xyz") == 1

  def readAllTypesTable(dataSourceFormat: String): DataFrame =
    spark.read.format(dataSourceFormat)
      .option("dataset", testDataset)
      .option("table", ALL_TYPES_TABLE_NAME)
      .load()


  Seq("bigquery", "com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
    .foreach(testsWithDataSource)

  def testsWithDataSource(dataSourceFormat: String) {

    test("write to bq - append save mode. DataSource %s".format(dataSourceFormat)) {
      // initial write
      writeToBigQuery(dataSourceFormat, initialData, SaveMode.Append)
      testTableNumberOfRows shouldBe 2
      initialDataValuesExist shouldBe true
      // second write
      writeToBigQuery(dataSourceFormat, additonalData, SaveMode.Append)
      testTableNumberOfRows shouldBe 4
      additionalDataValuesExist shouldBe true
    }

    test("write to bq - error if exists save mode. DataSource %s".format(dataSourceFormat)) {
      // initial write
      writeToBigQuery(dataSourceFormat, initialData, SaveMode.ErrorIfExists)
      testTableNumberOfRows shouldBe 2
      initialDataValuesExist shouldBe true
      // second write
      assertThrows[IllegalArgumentException] {
        writeToBigQuery(dataSourceFormat, additonalData, SaveMode.ErrorIfExists)
      }
    }

    test("write to bq - ignore save mode. DataSource %s".format(dataSourceFormat)) {
      // initial write
      writeToBigQuery(dataSourceFormat, initialData, SaveMode.Ignore)
      testTableNumberOfRows shouldBe 2
      initialDataValuesExist shouldBe true
      // second write
      writeToBigQuery(dataSourceFormat, additonalData, SaveMode.Ignore)
      testTableNumberOfRows shouldBe 2
      initialDataValuesExist shouldBe true
      additionalDataValuesExist shouldBe false
    }

    test("write to bq - overwrite save mode. DataSource %s".format(dataSourceFormat)) {
      // initial write
      writeToBigQuery(dataSourceFormat, initialData, SaveMode.Overwrite)
      testTableNumberOfRows shouldBe 2
      initialDataValuesExist shouldBe true
      // second write
      writeToBigQuery(dataSourceFormat, additonalData, SaveMode.Overwrite)
      testTableNumberOfRows shouldBe 2
      initialDataValuesExist shouldBe false
      additionalDataValuesExist shouldBe true
    }

    test("write to bq - orc format. DataSource %s".format(dataSourceFormat)) {
      // v2 does not support ORC
      if (dataSourceFormat.equals("bigquery")) {
        // required by ORC
        spark.conf.set("spark.sql.orc.impl", "native")
        writeToBigQuery(dataSourceFormat, initialData, SaveMode.ErrorIfExists, "orc")
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
      }
    }

    test("write to bq - avro format. DataSource %s".format(dataSourceFormat)) {
      writeToBigQuery(dataSourceFormat, initialData, SaveMode.ErrorIfExists, "avro")
      testTableNumberOfRows shouldBe 2
      initialDataValuesExist shouldBe true
    }

    test("write to bq - parquet format. DataSource %s".format(dataSourceFormat)) {
      // v2 does not support parquet
      if (dataSourceFormat.equals("bigquery")) {
        writeToBigQuery(dataSourceFormat, initialData, SaveMode.ErrorIfExists, "parquet")
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
      }
    }

    test("write to bq - simplified api. DataSource %s".format(dataSourceFormat)) {
      initialData.write.format(dataSourceFormat)
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .save(fullTableName)
      testTableNumberOfRows shouldBe 2
      initialDataValuesExist shouldBe true
    }

    test("write to bq - unsupported format. DataSource %s".format(dataSourceFormat)) {
      if (dataSourceFormat.equals("bigquery")) {
        assertThrows[Exception] {
          writeToBigQuery(dataSourceFormat, initialData, SaveMode.ErrorIfExists, "something else")
        }
      }
    }

    test("write all types to bq - avro format. DataSource %s".format(dataSourceFormat)) {
      val allTypesTable = readAllTypesTable(dataSourceFormat)
      writeToBigQuery(dataSourceFormat, allTypesTable, SaveMode.Overwrite, "avro")

      val df = spark.read.format(dataSourceFormat)
        .option("dataset", testDataset)
        .option("table", testTable)
        .load()

      assert(df.head() == allTypesTable.head())
      assert(df.schema == allTypesTable.schema)
    }

    test("query materialized view. DataSource %s".format(dataSourceFormat)) {
      var df = spark.read.format(dataSourceFormat)
        .option("table", "bigquery-public-data:ethereum_blockchain.live_logs")
        .option("viewsEnabled", "true")
        .option("viewMaterializationProject", System.getenv("GOOGLE_CLOUD_PROJECT"))
        .option("viewMaterializationDataset", testDataset)
        .load()
    }

    test("write to bq - adding the settings to spark.conf. DataSource %s"
      .format(dataSourceFormat)) {
      spark.conf.set("temporaryGcsBucket", temporaryGcsBucket)
      val df = initialData
      df.write.format(dataSourceFormat)
        .option("table", fullTableName)
        .save()
      testTableNumberOfRows shouldBe 2
      initialDataValuesExist shouldBe true
    }

    test("write to bq - partitioned and clustered table. DataSource %s".format(dataSourceFormat)) {
      val df = spark.read.format("com.google.cloud.spark.bigquery")
        .option("table", LIBRARIES_PROJECTS_TABLE)
        .load()
        .where("platform = 'Sublime'")

      df.write.format(dataSourceFormat)
        .option("table", fullTableNamePartitioned)
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("partitionField", "created_timestamp")
        .option("clusteredFields", "platform")
        .mode(SaveMode.Overwrite)
        .save()

      val tableDefinition = testPartitionedTableDefinition
      tableDefinition.getTimePartitioning.getField shouldBe "created_timestamp"
      tableDefinition.getClustering.getFields should contain("platform")
    }

    test("overwrite single partition. DataSource %s".format(dataSourceFormat)) {
      // create partitioned table
      val tableName = s"partitioned_table_$randomSuffix"
      val fullTableName = s"$testDataset.$tableName"
      bq.create(TableInfo.of(
        TableId.of(testDataset, tableName),
        StandardTableDefinition.newBuilder()
          .setSchema(Schema.of(
            Field.of("the_date", LegacySQLTypeName.DATE),
            Field.of("some_text", LegacySQLTypeName.STRING)
          ))
          .setTimePartitioning(TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
            .setField("the_date").build()).build()))
      // entering the data
      bq.query(QueryJobConfiguration.of(
        s"""
           |insert into `$fullTableName` (the_date, some_text) values
           |('2020-07-01', 'foo'),
           |('2020-07-02', 'bar')
           |""".stripMargin.replace('\n', ' ')))

      // overrding a single partition
      val newDataDF = spark.createDataFrame(
        List(Row(java.sql.Date.valueOf("2020-07-01"), "baz")).asJava,
        StructType(Array(
          StructField("the_date", DateType),
          StructField("some_text", StringType))))

      newDataDF.write.format(dataSourceFormat)
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("datePartition", "20200701")
        .mode("overwrite")
        .save(fullTableName)

      val result = spark.read.format(dataSourceFormat).load(fullTableName).collect()

      result.size shouldBe 2
      result.filter(row => row(1).equals("bar")).size shouldBe 1
      result.filter(row => row(1).equals("baz")).size shouldBe 1

    }

    //    test("support custom data types. DataSource %s".format(dataSourceFormat)) {
    //      val table = s"$testDataset.$testTable"
    //
    //      val originalVectorDF = spark.createDataFrame(
    //        List(Row("row1", 1, Vectors.dense(1, 2, 3))).asJava,
    //        StructType(Seq(
    //          StructField("name", DataTypes.StringType),
    //          StructField("num", DataTypes.IntegerType),
    //          StructField("vector", SQLDataTypes.VectorType))))
    //
    //      originalVectorDF.write.format(dataSourceFormat)
    //        // must use avro or orc
    //        .option("intermediateFormat", "avro")
    //        .option("temporaryGcsBucket", temporaryGcsBucket)
    //        .save(table)
    //
    //      val readVectorDF = spark.read.format(dataSourceFormat)
    //        .load(table)
    //
    //      val orig = originalVectorDF.head
    //      val read = readVectorDF.head
    //
    //      read should equal(orig)
    //    }

    test("compare read formats DataSource %s".format(dataSourceFormat)) {
      val allTypesTable = readAllTypesTable(dataSourceFormat)
      writeToBigQuery(dataSourceFormat, allTypesTable, SaveMode.Overwrite, "avro")

      val df = spark.read.format(dataSourceFormat)
        .option("dataset", testDataset)
        .option("table", testTable)
        .option("readDataFormat", "arrow")
        .load().cache()

      assert(df.head() == allTypesTable.head())

      // read from cache
      assert(df.head() == allTypesTable.head())
      assert(df.schema == allTypesTable.schema)
    }

    test("write to bq with description/comment. DataSource %s".format(dataSourceFormat)) {
      val testDescription = "test description"
      val testComment = "test comment"

      val metadata = Metadata
        .fromJson("{\"description\": \"" + testDescription + "\"}")

      val schemas = Seq(
        StructType(List(StructField("c1", IntegerType, true, metadata))),
        StructType(List(StructField("c1", IntegerType, true, Metadata.empty)
          .withComment(testComment))),
        StructType(List(StructField("c1", IntegerType, true, metadata)
          .withComment(testComment))),
        StructType(List(StructField("c1", IntegerType, true, Metadata.empty)))
      )

      val readValues = Seq(testDescription, testComment, testComment, null)

      for(i <- 0 until schemas.length){
        val data = Seq(Row(100), Row(200))

        val descriptionDF =
          spark.createDataFrame(spark.sparkContext.parallelize(data), schemas(i))

        writeToBigQuery(dataSourceFormat, descriptionDF, SaveMode.Overwrite)

        val readDF = spark.read.format(dataSourceFormat)
          .option("dataset", testDataset)
          .option("table", testTable)
          .load()

        val description = SchemaConverters
          .getDescriptionOrCommentOfField(readDF.schema(0))

        if(i <= schemas.length-2) {
          assert(description.isPresent)
          assert(description.orElse("").equals(readValues(i)))
        } else
          assert(!description.isPresent)
      }
    }
  }

  private def randomSuffix: String = {
    val uuid = UUID.randomUUID()
    java.lang.Long.toHexString(uuid.getMostSignificantBits) +
      java.lang.Long.toHexString(uuid.getLeastSignificantBits)
  }

  test("streaming bq write append") {
    failAfter(120 seconds) {
      val schema = initialData.schema
      val expressionEncoder: ExpressionEncoder[Row] =
        RowEncoder(schema).resolveAndBind()
      val stream = MemoryStream[Row](expressionEncoder, spark.sqlContext)
      var lastBatchId: Long = 0
      val streamingDF = stream.toDF()
      val cpLoc: String = "/tmp/%s-%d".
        format(fullTableName, System.nanoTime())
      // Start write stream
      val writeStream = streamingDF.writeStream.
        format("bigquery").
        outputMode(OutputMode.Append()).
        option("checkpointLocation", cpLoc).
        option("table", fullTableName).
        option("temporaryGcsBucket", temporaryGcsBucket).
        start

      // Write to stream
      stream.addData(initialData.collect())
      while (writeStream.lastProgress.batchId <= lastBatchId) {
        Thread.sleep(1000L)
      }
      lastBatchId = writeStream.lastProgress.batchId
      testTableNumberOfRows shouldBe 2
      initialDataValuesExist shouldBe true
      // Write to stream
      stream.addData(additonalData.collect())
      while (writeStream.lastProgress.batchId <= lastBatchId) {
        Thread.sleep(1000L)
      }
      writeStream.stop()
      testTableNumberOfRows shouldBe 4
      additionalDataValuesExist shouldBe true
    }
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
      .option("partitionField", "t")
      .option("partitionType", partitionType)
      .option("partitionRequireFilter", "true")
      .option("table", table)
      .save()

    val readDF = spark.read.format("bigquery").load(table)
    assert(readDF.count == 3)
  }
}

case class Data(str: String, t: java.sql.Timestamp)

