package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.SparkSession

trait SparkBigQueryPushdown {
  def supportsSparkVersion(sparkVersion: String): Boolean

  def enable(session: SparkSession, bigQueryStrategy: BigQueryStrategy): Unit

  def disable(session: SparkSession): Unit

  def createSparkExpressionConverter: SparkExpressionConverter
}
