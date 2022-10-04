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

class Spark33BigQueryPushdown extends BaseSparkBigQueryPushdown {

  override def supportsSparkVersion(sparkVersion: String): Boolean = {
    sparkVersion.startsWith("3.3")
  }

  override def createSparkExpressionConverter(expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory): SparkExpressionConverter = {
    new Spark33ExpressionConverter(expressionFactory, sparkPlanFactory)
  }

  override def createSparkExpressionFactory: SparkExpressionFactory = {
    new Spark33ExpressionFactory
  }

  override def createBigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory): BigQueryStrategy = {
    new Spark33BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory)
  }

  override def createSparkPlanFactory(): SparkPlanFactory = {
    new Spark33PlanFactory
  }
}
