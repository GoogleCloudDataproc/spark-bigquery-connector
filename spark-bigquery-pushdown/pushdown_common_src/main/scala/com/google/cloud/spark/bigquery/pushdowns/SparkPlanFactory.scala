package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan

class SparkPlanFactory {
  /**
   * Generate SparkPlan from the output and RDD of the translated query
   */
  def createSparkPlan(queryRoot: BigQuerySQLQuery, bigQueryRDDFactory: BigQueryRDDFactory): Option[SparkPlan] = {
    Some(BigQueryPlan(queryRoot.output, bigQueryRDDFactory.buildScanFromSQL(queryRoot.getStatement().toString)))
  }
}
