package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

class SparkPlanFactory {
  /**
   * Generate SparkPlan from the output and RDD of the translated query
   */
  def createBigQueryPlan(queryRoot: BigQuerySQLQuery, bigQueryRDDFactory: BigQueryRDDFactory): Option[BigQueryPlan] = {
    Some(BigQueryPlan(queryRoot.output, bigQueryRDDFactory.buildScanFromSQL(queryRoot.getStatement().toString)))
  }

  /**
   * Generate SparkPlan with ProjectExec node at the top of the plan
   */
  def createProjectPlan(projectList: Seq[NamedExpression], child: SparkPlan): Option[SparkPlan] = {
    Some(ProjectExec(projectList, child))
  }
}
