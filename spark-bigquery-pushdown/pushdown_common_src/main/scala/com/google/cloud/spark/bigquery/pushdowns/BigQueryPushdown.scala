package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.SparkSession

trait BigQueryPushdown {
  def supportsSparkVersion(sparkVersion: String): Boolean

  def enable(session: SparkSession): Unit

  def disable(session: SparkSession): Unit
}
