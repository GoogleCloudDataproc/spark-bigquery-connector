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
import com.google.cloud.bigquery.connector.common.{BigQueryClient, BigQueryUtil}
import com.google.cloud.http.BaseHttpServiceException
import com.google.cloud.spark.bigquery.SchemaConverters.getDescriptionOrCommentOfField
import com.google.cloud.spark.bigquery.SchemaConverters.toBigQuerySchema
import com.google.common.collect.ImmutableList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.JavaConverters._

case class BigQueryWriteHelper(bigQueryClient: BigQueryClient,
                               sqlContext: SQLContext,
                               saveMode: SaveMode,
                               options: SparkBigQueryConfig,
                               data: DataFrame,
                               tableExists: Boolean)
  extends Logging {

  val conf = sqlContext.sparkContext.hadoopConfiguration

  val gcsPath = SparkBigQueryUtil.createGcsPath(options, conf, sqlContext.sparkContext.applicationId)

  def writeDataFrameToBigQuery: Unit = {
    // If the CreateDisposition is CREATE_NEVER, and the table does not exist,
    // there's no point in writing the data to GCS in the first place as it going
    // to file on the BigQuery side.
    if (BigQueryUtilScala.toOption(options.getCreateDisposition)
      .map(cd => !tableExists && cd == CREATE_NEVER)
      .getOrElse(false)) {
      throw new IOException(
        s"""
           |For table ${friendlyTableName} Create Disposition is CREATE_NEVER and the table does
           |not exists. Aborting the insert""".stripMargin.replace('\n', ' '))
    }

    try {
      // based on pmkc's suggestion at https://git.io/JeWRt
      createTemporaryPathDeleter.map(Runtime.getRuntime.addShutdownHook(_))

      val format = options.getIntermediateFormat.getDataSource
      data.write.format(format).save(gcsPath.toString)

      loadDataToBigQuery
      updateMetadataIfNeeded
    } catch {
      case e: Exception => throw new RuntimeException("Failed to write to BigQuery", e)
    } finally {
      cleanTemporaryGcsPathIfNeeded
    }
  }

  def loadDataToBigQuery(): Unit = {
    val fs = gcsPath.getFileSystem(conf)
    val formatOptions = options.getIntermediateFormat.getFormatOptions
    val sourceUris = SparkBigQueryUtil.optimizeLoadUriListForSpark(
      ToIterator(fs.listFiles(gcsPath, false))
      .map(_.getPath.toString)
      .filter(_.toLowerCase.endsWith(
        s".${formatOptions.getType.toLowerCase}"))
      .toList
      .asJava)
    val writeDisposition = SparkBigQueryUtil.saveModeToWriteDisposition(saveMode)

    var sourceSchema: Schema = null
    if (options.schema.isPresent) {
      sourceSchema = toBigQuerySchema(options.schema.get())
    }
    bigQueryClient.loadDataIntoTable(options, sourceUris, formatOptions, writeDisposition, sourceSchema)
  }

  def friendlyTableName: String = BigQueryUtil.friendlyTableName(options.getTableId)

  def updateMetadataIfNeeded: Unit = {
    val fieldsToUpdate = data.schema
      .filter {
        field =>
          SupportedCustomDataType.of(field.dataType).isPresent ||
            getDescriptionOrCommentOfField(field).isPresent}
      .map (field => (field.name, field))
      .toMap

    if (!fieldsToUpdate.isEmpty) {
      logDebug(s"updating schema, found fields to update: ${fieldsToUpdate.keySet}")
      val originalTableInfo = bigQueryClient.getTable(options.getTableIdWithoutThePartition)
      val originalTableDefinition = originalTableInfo.getDefinition[TableDefinition]
      val originalSchema = originalTableDefinition.getSchema
      val updatedSchema = Schema.of(originalSchema.getFields.asScala.map(field => {
        fieldsToUpdate.get(field.getName)
          .map(dataField => updatedField(field, dataField))
          .getOrElse(field)
      }).asJava)
      val updatedTableInfo = originalTableInfo.toBuilder.setDefinition(
        originalTableDefinition.toBuilder.setSchema(updatedSchema).build
      )
      bigQueryClient.update(updatedTableInfo.build)
    }
  }

  def updatedField(field: Field, dataField: StructField): Field = {
    val newField = field.toBuilder
    val bqDescription = getDescriptionOrCommentOfField(dataField)

    if(bqDescription.isPresent){
      newField.setDescription(bqDescription.get)
    } else {
      val marker = SupportedCustomDataType.of(dataField.dataType).get.getTypeMarker
      val description = field.getDescription
      if (description == null) {
        newField.setDescription(marker)
      } else if (!description.endsWith(marker)) {
        newField.setDescription(s"${description} ${marker}")
      }
    }

    newField.build
  }

  def cleanTemporaryGcsPathIfNeeded: Unit = {
    // TODO(davidrab): add flag to disable the deletion?
    createTemporaryPathDeleter.map(_.deletePath)
  }

  private def createTemporaryPathDeleter: scala.Option[IntermediateDataCleaner] =
    BigQueryUtilScala.toOption(options.getTemporaryGcsBucket)
      .map(_ => IntermediateDataCleaner(gcsPath, conf))

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
