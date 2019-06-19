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
import com.google.cloud.bigquery.TableId
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType

/** Options for defining {@link BigQueryRelation}s */
case class SparkBigQueryOptions(
    tableId: TableId,
    parentProject: String,
    serviceAccountKeyString: Option[String] = None,
    serviceAccountKeyFile: Option[String] = None,
    filter: Option[String] = None,
    schema: Option[StructType] = None,
    skewLimit: Double = SparkBigQueryOptions.SKEW_LIMIT_DEFAULT,
    parallelism: Option[Int] = None) {

  def createCredentials: Option[Credentials] =
    (serviceAccountKeyString, serviceAccountKeyFile) match {
      case (Some(key), None) =>
        Some(GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.decodeBase64(key))))
      case (None, Some(file)) =>
        Some(GoogleCredentials.fromStream(new FileInputStream(file)))
      case (None, None) =>
        None
      case (Some(_), Some(_)) =>
        throw new IllegalArgumentException("Service Account Key can be provided through file " +
          "name or a Base64 string directly, but not both!")
    }
}

/** Resolvers for {@link SparkBigQueryOptions} */
object SparkBigQueryOptions {
  private val SKEW_LIMIT_KEY = "skewLimit"
  private val SKEW_LIMIT_DEFAULT = 1.5

  def apply(
      parameters: Map[String, String],
      allConf: Map[String, String],
      hadoopConf: Configuration,
      schema: Option[StructType],
      defaultBilledProject: Option[String])
  : SparkBigQueryOptions = {
    val tableParam = getRequiredOption(parameters, "table")
    val datasetParam = getOption(parameters, "dataset")
    val projectParam = getOption(parameters, "project")
    val serviceAccountKeyStringParam = getAnyOption(allConf, parameters, "serviceAccountKeyString")
    val serviceAccountKeyFileParam = getAnyOption(allConf, parameters, "serviceAccountKeyFile")
    val tableId = BigQueryUtil.parseTableId(tableParam, datasetParam, projectParam)
    val parentProject = getRequiredOption(parameters, "parentProject",
      defaultBilledProject)
    val filter = getOption(parameters, "filter")
    val parallelism = getOption(parameters, "parallelism").map(_.toInt)
    val skewLimit = getOption(parameters, SKEW_LIMIT_KEY).map(_.toDouble)
        .getOrElse(SKEW_LIMIT_DEFAULT)
    // BigQuery will actually error if we close the last stream.
    assert(skewLimit >= 1.0,
      s"Paramater '$SKEW_LIMIT_KEY' must be at least 1.0 to read all data '$skewLimit'")
    SparkBigQueryOptions(tableId, parentProject, serviceAccountKeyStringParam,
      serviceAccountKeyFileParam, filter, schema, skewLimit, parallelism)
  }


  private def getRequiredOption(
      options: Map[String, String],
      name: String,
      fallback: Option[String] = None): String = {
    getOption(options, name, fallback)
        .getOrElse(sys.error(s"Option $name required."))
  }

  private def getOption(
      options: Map[String, String],
      name: String,
      fallback: Option[String] = None): Option[String] = {
    options.get(name.toLowerCase).orElse(fallback)
  }

  private def getAnyOption(globalOptions: Map[String, String],
                           options: Map[String, String],
                           name: String): Option[String] =
    options.get(name.toLowerCase).orElse(globalOptions.get(name.toLowerCase))
}

