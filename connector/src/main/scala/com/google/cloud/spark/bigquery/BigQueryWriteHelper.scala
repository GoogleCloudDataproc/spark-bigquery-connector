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

import java.io.IOException
import java.util.UUID

import com.google.cloud.bigquery.JobInfo.CreateDisposition.CREATE_NEVER
import com.google.cloud.bigquery._
import com.google.cloud.http.BaseHttpServiceException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.JavaConverters._

case class BigQueryWriteHelper(bigQuery: BigQuery,
                               sqlContext: SQLContext,
                               saveMode: SaveMode,
                               options: SparkBigQueryOptions,
                               data: DataFrame,
                               tableExists: Boolean)
  extends Logging {

  val conf = sqlContext.sparkContext.hadoopConfiguration

  val temporaryGcsPath = {
    var needNewPath = true
    var gcsPath: Path = null
    val applicationId = sqlContext.sparkContext.applicationId

    while (needNewPath) {
      val gcsPathOption = options.temporaryGcsBucket.map(bucket =>
        s"gs://$bucket/.spark-bigquery-${applicationId}-${UUID.randomUUID()}")
      require(gcsPathOption.isDefined, "Temporary GCS path has not been set")
      gcsPath = new Path(gcsPathOption.get)
      val fs = gcsPath.getFileSystem(conf)
      needNewPath = fs.exists(gcsPath) // if teh path exists for some reason, then retry
    }

    gcsPath
  }

  def writeDataFrameToBigQuery: Unit = {
    // If the CreateDisposition is CREATE_NEVER, and the table does not exist,
    // there's no point in writing the data to GCS in teh first place as it going
    // to file on the BigQuery side.
    if (options.createDisposition.map(cd => !tableExists && cd == CREATE_NEVER).getOrElse(false)) {
      throw new IOException(
        s"""
           |For table ${BigQueryUtil.friendlyTableName(options.tableId)}
           |Create Disposition is CREATE_NEVER and the table does not exists.
           |Aborting the insert""".stripMargin.replace('\n', ' '))
    }

    try {
      // based on pmkc's suggestion at https://git.io/JeWRt
      Runtime.getRuntime.addShutdownHook(createTemporaryPathDeleter)

      val format = options.intermediateFormat.dataSource
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
      .filter(_.toLowerCase.endsWith(
        s".${options.intermediateFormat.formatOptions.getType.toLowerCase}"))
      .toList
      .asJava

    val jobConfigurationBuilder = LoadJobConfiguration.newBuilder(
      options.tableId, sourceUris, options.intermediateFormat.formatOptions)
      .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
      .setWriteDisposition(saveModeToWriteDisposition(saveMode))
      .setAutodetect(true)

    if (options.createDisposition.isDefined) {
      jobConfigurationBuilder.setCreateDisposition(options.createDisposition.get)
    }

    if (options.partitionField.isDefined || options.partitionType.isDefined) {
      val timePartitionBuilder = TimePartitioning.newBuilder(
        TimePartitioning.Type.valueOf(options.partitionType.getOrElse("DAY"))
      )

      if (options.partitionExpirationMs.isDefined) {
        timePartitionBuilder.setExpirationMs(options.partitionExpirationMs.get)
      }

      if (options.partitionRequireFilter.isDefined) {
        timePartitionBuilder.setRequirePartitionFilter(options.partitionRequireFilter.get)
      }

      if (options.partitionField.isDefined) {
        timePartitionBuilder.setField(options.partitionField.get)
      }

      jobConfigurationBuilder.setTimePartitioning(timePartitionBuilder.build())

      if (options.clusteredFields.isDefined) {
        val clustering =
          Clustering.newBuilder().setFields(options.clusteredFields.get.toList.asJava).build();
        jobConfigurationBuilder.setClustering(clustering)
      }
    }

    if (!options.loadSchemaUpdateOptions.isEmpty) {
      jobConfigurationBuilder.setSchemaUpdateOptions(options.loadSchemaUpdateOptions)
    }

    val jobConfiguration = jobConfigurationBuilder.build

    val jobInfo = JobInfo.of(jobConfiguration)
    val job = bigQuery.create(jobInfo)

    logInfo(s"Submitted load to ${options.tableId}. jobId: ${job.getJobId}")
    // TODO(davidrab): add retry options
    val finishedJob = job.waitFor()
    if (finishedJob.getStatus.getError != null) {
      throw new BigQueryException(
        BaseHttpServiceException.UNKNOWN_CODE,
        s"""Failed to load to ${friendlyTableName} in job ${job.getJobId}. BigQuery error was
           |${finishedJob.getStatus.getError.getMessage}""".stripMargin.replace('\n', ' '),
        finishedJob.getStatus.getError)
    } else {
      logInfo(s"Done loading to ${friendlyTableName}. jobId: ${job.getJobId}")
    }
  }

  def saveModeToWriteDisposition(saveMode: SaveMode): JobInfo.WriteDisposition = saveMode match {
    case SaveMode.Append => JobInfo.WriteDisposition.WRITE_APPEND
    case SaveMode.Overwrite => JobInfo.WriteDisposition.WRITE_TRUNCATE
    case unsupported => throw new UnsupportedOperationException(s"SaveMode $unsupported is currently not supported.")
  }

  def friendlyTableName: String = BigQueryUtil.friendlyTableName(options.tableId)

  def cleanTempraryGcsPath: Unit = {
    // TODO(davidrab): add flag to disable the deletion?
    createTemporaryPathDeleter.deletePath
  }

  private def createTemporaryPathDeleter = IntermediateDataCleaner(temporaryGcsPath, conf)

  def verifySaveMode: Unit = {
    if (saveMode == SaveMode.ErrorIfExists || saveMode == SaveMode.Ignore) {
      throw new UnsupportedOperationException(s"SaveMode $saveMode is not supported")
    }
  }
}

/**
 * Responsible for recursively deleting the intermediate path.
 * Implementing Thread in order to act as shutdown hook.
 *
 * @param path the path to delete
 * @param conf the hadoop configuration
 */
case class IntermediateDataCleaner(path: Path, conf: Configuration)
  extends Thread with Logging {

  override def run: Unit = deletePath

  def deletePath: Unit =
    try {
      val fs = path.getFileSystem(conf)
      if (pathExists(fs, path)) {
        fs.delete(path, true)
      }
    } catch {
      case e: Exception => logError(s"Failed to delete path $path", e)
    }

  // fs.exists can throw exception on missing path
  private def pathExists(fs: FileSystem, path: Path): Boolean = {
    try {
      fs.exists(path)
    } catch {
      case e: Exception => false
    }
  }
}

/**
 * Converts HDFS RemoteIterator to Scala iterator
 */
case class ToIterator[E](remote: RemoteIterator[E]) extends Iterator[E] {
  override def hasNext: Boolean = remote.hasNext

  override def next(): E = remote.next()
}
