package com.google.cloud.spark.bigquery

import java.util.UUID

import collection.JavaConverters._
import com.google.cloud.bigquery.{BigQuery, BigQueryException, FormatOptions, JobInfo, LoadJobConfiguration}
import com.google.cloud.http.BaseHttpServiceException
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.{Path, RemoteIterator}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

case class BigQueryWriteHelper(bigQuery:BigQuery,
                               sqlContext: SQLContext,
                               saveMode: SaveMode,
                               options: SparkBigQueryOptions,
                               data: DataFrame) extends StrictLogging {

  import org.apache.spark.sql.SaveMode

  val conf = sqlContext.sparkSession.sparkContext.hadoopConfiguration

  val temporaryGcsPath = {

    val gcsPathOption = options.temporaryGcsBucket.map(bucket =>
      s"gs://$bucket/.spark-bigquery-${System.currentTimeMillis()}-${UUID.randomUUID()}")
    require(gcsPathOption.isDefined, "Temporary GCS path has not been set")
    val gcsPath = new Path(gcsPathOption.get)
    val fs = gcsPath.getFileSystem(conf)
    if (fs.exists(gcsPath)) {
      throw new IllegalStateException(s"Conflict occurred creating export directory. Path %gcsPath already exists")
    }
    gcsPath
  }

  def writeDataFrameToBigQuery : Unit = {
    try {
      val format = options.intermediateFormat.getType.toLowerCase
      data.write.format(format).save(temporaryGcsPath.toString)
      loadDataToBigQuery
    } catch {
      case e:Exception => throw new RuntimeException("Failed to write to BigQuery", e)
    } finally {
      cleanTempraryGcsPath
    }
  }

  val saveModeToWriteDisposition = Map(
    SaveMode.Append -> JobInfo.WriteDisposition.WRITE_APPEND,
    SaveMode.Overwrite -> JobInfo.WriteDisposition.WRITE_TRUNCATE)

  def verifySaveMode = {
    if (saveMode == SaveMode.ErrorIfExists || saveMode == SaveMode.Ignore) {
      throw new UnsupportedOperationException(s"SaveMode $saveMode is not supported")
    }
  }

  def loadDataToBigQuery(): Unit = {
    val fs = temporaryGcsPath.getFileSystem(conf)
    val sourceUris = ToIterator(fs.listFiles(temporaryGcsPath, false))
      .map(_.getPath.toString)
      .filter(! _.endsWith("_SUCCESS"))
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
        s"Failed to load to ${friendlyTableName} in job ${job.getJobId}. BigQuery error was ${finishedJob.getStatus.getError.getMessage}",
        finishedJob.getStatus.getError)
    } else {
      logger.info(s"Done loading to ${friendlyTableName}. jobId: ${job.getJobId}")
    }
  }

  def friendlyTableName = BigQueryUtil.friendlyTableName(options.tableId)

  def cleanTempraryGcsPath: Unit = {
    //TODO(davidrab): add flag to disable the deletion?
    val fs = temporaryGcsPath.getFileSystem(conf)
    fs.delete(temporaryGcsPath, true)
  }


}

case class ToIterator[E](remote:RemoteIterator[E]) extends Iterator[E] {
  override def hasNext: Boolean = remote.hasNext
  override def next(): E = remote.next()
}