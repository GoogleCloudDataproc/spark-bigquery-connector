/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.SparkSession

abstract class BaseSparkBigQueryPushdown extends SparkBigQueryPushdown {

  def supportsSparkVersion(sparkVersion: String): Boolean

  override def enable(session: SparkSession): Unit = {
    val sparkExpressionFactory = createSparkExpressionFactory
    val sparkPlanFactory = createSparkPlanFactory
    val sparkExpressionConverter = createSparkExpressionConverter(
      sparkExpressionFactory, sparkPlanFactory)
    val bigQueryStrategy = createBigQueryStrategy(
      sparkExpressionConverter, sparkExpressionFactory, sparkPlanFactory)
    SparkBigQueryPushdownUtil.enableBigQueryStrategy(session, bigQueryStrategy)
  }

  override def disable(session: SparkSession): Unit = {
    SparkBigQueryPushdownUtil.disableBigQueryStrategy(session)
  }

  def createSparkExpressionConverter(expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory): SparkExpressionConverter

  def createSparkExpressionFactory: SparkExpressionFactory

  def createSparkPlanFactory(): SparkPlanFactory

  def createBigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory): BigQueryStrategy
}
