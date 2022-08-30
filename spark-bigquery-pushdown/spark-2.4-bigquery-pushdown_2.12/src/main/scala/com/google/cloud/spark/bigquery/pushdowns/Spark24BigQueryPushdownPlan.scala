package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.doExecuteSparkPlan
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

/** BigQueryPlan, with RDD defined by custom query. */
case class Spark24BigQueryPushdownPlan(output: Seq[Attribute], rdd: RDD[InternalRow])
  extends SparkPlan {

  override def children: Seq[SparkPlan] = Nil

  protected override def doExecute(): RDD[InternalRow] = {
    doExecuteSparkPlan(output, rdd)
  }
}
