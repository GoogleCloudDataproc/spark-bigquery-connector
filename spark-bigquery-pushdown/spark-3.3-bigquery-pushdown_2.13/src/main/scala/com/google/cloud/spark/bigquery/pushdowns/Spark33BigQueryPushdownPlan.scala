package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.bigquery.connector.common.BigQueryConnectorException
import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.doExecuteSparkPlan
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

/** BigQueryPlan, with RDD defined by custom query. */
case class Spark33BigQueryPushdownPlan(output: Seq[Attribute], rdd: RDD[InternalRow])
  extends SparkPlan {

  override def children: Seq[SparkPlan] = Nil

  protected override def doExecute(): RDD[InternalRow] = {
    doExecuteSparkPlan(output, rdd)
  }

  // New function introduced in Spark 3.2
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    if (newChildren.nonEmpty) {
      throw new BigQueryConnectorException("Spark connector internal error: " +
        "Spark32BigQueryPushdownPlan.withNewChildrenInternal() is called to set some children nodes.")
    }
    this
  }
}
