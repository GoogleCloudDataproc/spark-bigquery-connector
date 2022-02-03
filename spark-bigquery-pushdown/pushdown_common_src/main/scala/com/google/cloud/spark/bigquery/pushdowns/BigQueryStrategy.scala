package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

class BigQueryStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    throw new NotImplementedError("BigQueryStrategy has not been implemented yet")
  }
}
