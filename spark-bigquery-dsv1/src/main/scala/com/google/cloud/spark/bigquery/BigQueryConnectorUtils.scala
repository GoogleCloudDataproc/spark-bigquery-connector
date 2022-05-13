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

package com.google.cloud.spark.bigquery

import com.google.cloud.spark.bigquery.pushdowns.{BigQueryStrategy, SparkBigQueryPushdown}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import java.util.ServiceLoader

object BigQueryConnectorUtils {
  def enablePushdownSession(session: SparkSession): Unit = {
    // Find the supported implementation based on Spark version and enable pushdown
    val sparkBigQueryPushdown = ServiceLoader.load(classOf[SparkBigQueryPushdown])
      .iterator().asScala.find(p => p.supportsSparkVersion(session.version))
      .getOrElse(sys.error(s"Query pushdown not supported for Spark version ${session.version}"))

    sparkBigQueryPushdown.enable(session, new BigQueryStrategy(sparkBigQueryPushdown.createSparkExpressionConverter, sparkBigQueryPushdown.createSparkExpressionFactory, sparkBigQueryPushdown.createSparkPlanFactory))
  }

  def disablePushdownSession(session: SparkSession): Unit = {
    // Find the supported implementation based on Spark version and disable pushdown
    val sparkBigQueryPushdown = ServiceLoader.load(classOf[SparkBigQueryPushdown])
      .iterator().asScala.find(p => p.supportsSparkVersion(session.version))
      .getOrElse(sys.error(s"Query pushdown not supported for Spark version ${session.version}"))
    sparkBigQueryPushdown.disable(session)
  }
}
