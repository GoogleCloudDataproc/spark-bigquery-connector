/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory
import org.apache.spark.sql.execution.SparkPlan

class Spark33PlanFactory extends SparkPlanFactory {
  /**
   * Generate SparkPlan from the output and RDD of the translated query
   */
  override def createBigQueryPlan(queryRoot: BigQuerySQLQuery, bigQueryRDDFactory: BigQueryRDDFactory): Option[SparkPlan] = {
    Some(Spark33BigQueryPushdownPlan(queryRoot.output, bigQueryRDDFactory.buildScanFromSQL(queryRoot.getStatement().toString)))
  }
}
