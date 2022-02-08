package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.SparkSession

class Spark31BigQueryPushdown extends SparkBigQueryPushdown {
  override def enable(session: SparkSession): Unit = {
    // TODO
  }

  override def disable(session: SparkSession): Unit = {
    // TODO
  }

  override def supportsSparkVersion(sparkVersion: String): Boolean = {
    sparkVersion.startsWith("3.1")
  }
}
