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

import java.io.{ByteArrayInputStream, FileInputStream}

import com.google.api.client.util.Base64
import com.google.auth.Credentials
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.{BigQueryOptions, FormatOptions, TableId}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType

/** Options for defining {@link BigQueryRelation}s */
case class SparkBigQueryOptions(
  tableId: TableId,
  parentProject: String,
  credentials: Option[String] = None,
  credentialsFile: Option[String] = None,
  filter: Option[String] = None,
  schema: Option[StructType] = None,
  parallelism: Option[Int] = None,
  temporaryGcsBucket: Option[String] = None,
  intermediateFormat: FormatOptions = SparkBigQueryOptions.DefaultFormat,
  combinePushedDownFilters: Boolean = true,
  viewsEnabled: Boolean = false,
  viewMaterializationProject: Option[String] = None,
  viewMaterializationDataset: Option[String] = None,
  viewExpirationTimeInHours:Int = 24,
  maxReadRowsRetries:Int = 3) {

  def createCredentials: Option[Credentials] =
    (credentials, credentialsFile) match {
      case (Some(key), None) =>
        Some(GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.decodeBase64(key))))
      case (None, Some(file)) =>
        Some(GoogleCredentials.fromStream(new FileInputStream(file)))
      case (None, None) =>
        None
      case (Some(_), Some(_)) =>
        throw new IllegalArgumentException("Only one of credentials or credentialsFile can be" +
          " specified in the options.")
    }

}

/** Resolvers for {@link SparkBigQueryOptions} */
object SparkBigQueryOptions {

  val IntermediateFormatOption = "intermediateFormat"
  val ViewsEnabledOption = "viewsEnabled"

  val DefaultFormat = FormatOptions.parquet()
  private val PermittedIntermediateFormats =
    Set(FormatOptions.orc(), FormatOptions.parquet())

  def apply(
     parameters: Map[String, String],
     allConf: Map[String, String],
     hadoopConf: Configuration,
     schema: Option[StructType])
  : SparkBigQueryOptions = {
    val tableParam = getRequiredOption(parameters, "table")
    val datasetParam = getOption(parameters, "dataset")
    val projectParam = getOption(parameters, "project")
    val credsParam = getAnyOption(allConf, parameters, "credentials")
    val credsFileParam = getAnyOption(allConf, parameters, "credentialsFile")
    val tableId = BigQueryUtil.parseTableId(tableParam, datasetParam, projectParam)
    val parentProject = getRequiredOption(parameters, "parentProject",
      defaultBilledProject)
    val filter = getOption(parameters, "filter")
    val parallelism = getOption(parameters, "parallelism").map(_.toInt)
    val temporaryGcsBucket = getAnyOption(allConf, parameters, "temporaryGcsBucket")
    val intermediateFormat = getAnyOption(allConf, parameters, IntermediateFormatOption)
      .map(s => FormatOptions.of(s.toUpperCase))
      .getOrElse(DefaultFormat)
    if (!PermittedIntermediateFormats.contains(intermediateFormat)) {
      throw new IllegalArgumentException(
        s"""Intermediate format '${intermediateFormat.getType}' is not supported.
           |Supported formats are ${PermittedIntermediateFormats.map(_.getType)}"""
          .stripMargin.replace('\n', ' '))
    }
    val combinePushedDownFilters = getAnyBooleanOption(
      allConf, parameters, "combinePushedDownFilters", true)
    val viewsEnabled = getAnyBooleanOption(
      allConf, parameters, ViewsEnabledOption, false)
    val viewMaterializationProject =
      getAnyOption(allConf, parameters, "viewMaterializationProject")
    val viewMaterializationDataset =
      getAnyOption(allConf, parameters, "viewMaterializationDataset")

    SparkBigQueryOptions(tableId, parentProject, credsParam, credsFileParam,
      filter, schema, parallelism, temporaryGcsBucket, intermediateFormat,
      combinePushedDownFilters, viewsEnabled, viewMaterializationProject,
      viewMaterializationDataset)
  }

  private def defaultBilledProject = () =>
    Some(BigQueryOptions.getDefaultInstance.getProjectId)

  private def getRequiredOption(
      options: Map[String, String],
      name: String,
      fallback: () => Option[String] = () => None): String = {
    getOption(options, name, fallback)
        .getOrElse(sys.error(s"Option $name required."))
  }

  private def getOption(
      options: Map[String, String],
      name: String,
      fallback: () => Option[String] = () => None): Option[String] = {
    options.get(name).orElse(fallback())
  }

  private def getAnyOption(globalOptions: Map[String, String],
                           options: Map[String, String],
                           name: String): Option[String] =
    options.get(name).orElse(globalOptions.get(name))

  private def getAnyBooleanOption(globalOptions: Map[String, String],
                                  options: Map[String, String],
                                  name: String,
                                  defaultValue: Boolean): Boolean =
    getAnyOption(globalOptions, options, ViewsEnabledOption)
      .map(_.toBoolean)
      .getOrElse(defaultValue)
}

