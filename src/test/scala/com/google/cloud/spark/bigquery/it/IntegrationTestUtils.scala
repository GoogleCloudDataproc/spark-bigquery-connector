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

import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption
import com.google.cloud.bigquery._
import com.typesafe.scalalogging.Logger

object IntegrationTestUtils {

  private val log: Logger = Logger(getClass)

  def getBigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService

  def createDataset(dataset: String): Unit = {
    val bq = getBigquery
    val datasetId = DatasetId.of(dataset)
    log.warn(s"Creating test dataset: $datasetId")
    bq.create(DatasetInfo.of(datasetId))
  }

  def runQuery(query: String): Unit = {
    log.warn(s"Running query '$query'")
    getBigquery.query(QueryJobConfiguration.of(query))
  }

  def deleteDatasetAndTables(dataset: String): Unit = {
    val bq = getBigquery
    log.warn(s"Deleting test dataset '$dataset' and its contents")
    bq.delete(DatasetId.of(dataset), DatasetDeleteOption.deleteContents())
  }
}
