package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.SparkSession

class BigQueryPushdownService extends BigQueryPushdown {
  override def enable(session: SparkSession): Unit = {
    // TODO
  }

  override def disable(session: SparkSession): Unit = {
    // TODO
  }

  override def supportsSparkVersion(sparkVersion: String): Boolean = {
    sparkVersion.startsWith("2.4")
  }
}
