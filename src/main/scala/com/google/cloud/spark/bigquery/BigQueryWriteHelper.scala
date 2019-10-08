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

import java.util.UUID

import com.google.cloud.bigquery.{BigQuery, BigQueryException, JobInfo, LoadJobConfiguration}
import com.google.cloud.http.BaseHttpServiceException
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, RemoteIterator}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.JavaConverters._

case class BigQueryWriteHelper(bigQuery: BigQuery,
                               sqlContext: SQLContext,
                               saveMode: SaveMode,
                               options: SparkBigQueryOptions,
                               data: DataFrame) extends StrictLogging {

  import org.apache.spark.sql.SaveMode

  val conf = sqlContext.sparkSession.sparkContext.hadoopConfiguration

  val temporaryGcsPath = {
    verifyThatGcsConnectorIsInstalled

    var needNewPath = true
    var gcsPath: Path = null

    while (needNewPath) {
      val gcsPathOption = options.temporaryGcsBucket.map(bucket =>
        s"gs://$bucket/.spark-bigquery-${System.currentTimeMillis()}-${UUID.randomUUID()}")
      require(gcsPathOption.isDefined, "Temporary GCS path has not been set")
      gcsPath = new Path(gcsPathOption.get)
      val fs = gcsPath.getFileSystem(conf)
      needNewPath = fs.exists(gcsPath) // if teh path exists for some reason, then retry
    }

    gcsPath
  }

  val saveModeToWriteDisposition = Map(
    SaveMode.Append -> JobInfo.WriteDisposition.WRITE_APPEND,
    SaveMode.Overwrite -> JobInfo.WriteDisposition.WRITE_TRUNCATE)

  def writeDataFrameToBigQuery: Unit = {
    try {
      // based on pmkc's suggestion at https://git.io/JeWRt
      Runtime.getRuntime.addShutdownHook(createTemporaryPathDeleter)

      val format = options.intermediateFormat.getType.toLowerCase
      data.write.format(format).save(temporaryGcsPath.toString)
      loadDataToBigQuery
    } catch {
      case e: Exception => throw new RuntimeException("Failed to write to BigQuery", e)
    } finally {
      cleanTempraryGcsPath
    }
  }

  def loadDataToBigQuery(): Unit = {
    val fs = temporaryGcsPath.getFileSystem(conf)
    val sourceUris = ToIterator(fs.listFiles(temporaryGcsPath, false))
      .map(_.getPath.toString)
      .filter(!_.endsWith("_SUCCESS"))
      .toList
      .asJava

    val jobConfiguration = LoadJobConfiguration.newBuilder(
      options.tableId, sourceUris, options.intermediateFormat)
      .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
      .setWriteDisposition(saveModeToWriteDisposition(saveMode))
      .build

    val jobInfo = JobInfo.of(jobConfiguration)
    val job = bigQuery.create(jobInfo)

    logger.info(s"Submitted load to ${options.tableId}. jobId: ${job.getJobId}")
    // TODO(davidrab): add retry options
    val finishedJob = job.waitFor()
    if (finishedJob.getStatus.getError != null) {
      throw new BigQueryException(
        BaseHttpServiceException.UNKNOWN_CODE,
        s"""Failed to load to ${friendlyTableName} in job ${job.getJobId}. BigQuery error was
           |${finishedJob.getStatus.getError.getMessage}""".stripMargin.replace('\n', ' '),
        finishedJob.getStatus.getError)
    } else {
      logger.info(s"Done loading to ${friendlyTableName}. jobId: ${job.getJobId}")
    }
  }

  def friendlyTableName: String = BigQueryUtil.friendlyTableName(options.tableId)

  def cleanTempraryGcsPath: Unit = {
    // TODO(davidrab): add flag to disable the deletion?
    createTemporaryPathDeleter.deletePath
  }

  private def createTemporaryPathDeleter = HdfsPathDeleter(temporaryGcsPath, conf)

  def verifySaveMode: Unit = {
    if (saveMode == SaveMode.ErrorIfExists || saveMode == SaveMode.Ignore) {
      throw new UnsupportedOperationException(s"SaveMode $saveMode is not supported")
    }
  }

  def verifyThatGcsConnectorIsInstalled: Unit = {
    try {
      val path = new Path("gs://test/")
      path.getFileSystem(conf)
    } catch {
      case e: Exception => throw new IllegalStateException(
        """It seems that the Hadoop GCS connector it not installed or not
          |configured properly. Please see
          |https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage
          |for more details""".stripMargin.replace('\n', ' '))
    }
  }

}

/**
 * Resposible for recursively deleting an HDFS path. Implementing Thread in order
 * to act as shutdown hook
 * @param path the path to delete
 * @param conf the hadoop configuration
 */
case class HdfsPathDeleter(path: Path, conf: Configuration) extends Thread with StrictLogging {
  def deletePath: Unit =
    try {
      val fs = path.getFileSystem(conf)
      fs.delete(path, true)
    } catch {
      case e: Exception => logger.error(s"Failed to delete path $path", e)
    }

  override def run : Unit = deletePath
}

/**
  * Converts HDFS RemoteIterator to Scala iterator
  */
case class ToIterator[E](remote: RemoteIterator[E]) extends Iterator[E] {
  override def hasNext: Boolean = remote.hasNext

  override def next(): E = remote.next()
}
