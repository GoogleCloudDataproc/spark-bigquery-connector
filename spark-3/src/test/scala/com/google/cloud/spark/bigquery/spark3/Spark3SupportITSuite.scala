package com.google.cloud.spark.bigquery.spark3

import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption
import com.google.cloud.bigquery.{BigQueryOptions, DatasetId, QueryJobConfiguration}
import com.google.common.base.Preconditions
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.concurrent.TimeLimits
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatest.time.SpanSugar._

class Spark3SupportITSuite extends FunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll
  with Matchers
  with TimeLimits
  with TableDrivenPropertyChecks {

  private var testDataset: String = _
  private var spark: SparkSession = _
  private var testTable: String = _
  val bq = BigQueryOptions.getDefaultInstance.getService
  val temporaryGcsBucket = Preconditions.checkNotNull(
    System.getenv("TEMPORARY_GCS_BUCKET"),
    "Please set the %s env variable to point to a write enabled GCS bucket",
    "TEMPORARY_GCS_BUCKET")

  before {
    // have a fresh table for each test
    testTable = s"test_${System.nanoTime()}"
  }

  override def beforeAll: Unit = {
    spark = SparkSession.builder()
      .appName("Spark3SupportITSuite")
      .master("local")
      .getOrCreate()

    testDataset = s"spark3_support_bigquery_it_${System.currentTimeMillis()}"
  }

  private def fullTableName = s"$testDataset.$testTable"
  private def testTableNumberOfRows = bq.getTable(testDataset, testTable).getNumRows.intValue

  private def initialData =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(CustomRow("one", "two"))))

  private def additonalData =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(CustomRow("three", "four"))))

  private def initialDataValuesExist = numberOfRowsWith("one") == 1

  private def additionalDataValuesExist = numberOfRowsWith("three") == 1

  private def numberOfRowsWith(value: String) =
    bq.query(QueryJobConfiguration.of(s"select c1 from $fullTableName where c1='$value'"))
      .getTotalRows

  test("streaming bq write append for spark3") {
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
      testTableNumberOfRows shouldBe 1
      initialDataValuesExist shouldBe true

      // Write to stream
      stream.addData(additonalData.collect())
      while (writeStream.lastProgress.batchId <= lastBatchId) {
        Thread.sleep(1000L)
      }

      writeStream.stop()
      testTableNumberOfRows shouldBe 2
      additionalDataValuesExist shouldBe true
    }
  }

  override def afterAll: Unit = {
    bq.delete(DatasetId.of(testDataset), DatasetDeleteOption.deleteContents())
  }

}

case class CustomRow(c1: String, c2: String)