package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

trait SparkPlanFactory {
  /**
   * Generate SparkPlan from the output and RDD of the translated query
   */
  def createBigQueryPlan(queryRoot: BigQuerySQLQuery, bigQueryRDDFactory: BigQueryRDDFactory): Option[SparkPlan]

  def createProjectPlan(projectNode: Project, child: SparkPlan): Option[SparkPlan] = {
    Some(ProjectExec(projectNode.projectList, child))
  }
}
