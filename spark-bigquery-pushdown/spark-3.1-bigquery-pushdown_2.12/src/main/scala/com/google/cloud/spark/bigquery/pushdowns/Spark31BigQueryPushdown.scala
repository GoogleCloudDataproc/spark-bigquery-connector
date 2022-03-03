package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.SparkSession

class Spark31BigQueryPushdown extends SparkBigQueryPushdown {
  override def enable(session: SparkSession, bigQueryStrategy: BigQueryStrategy): Unit = {
    SparkBigQueryPushdownUtil.enableBigQueryStrategy(session, bigQueryStrategy)
  }

  override def disable(session: SparkSession): Unit = {
    SparkBigQueryPushdownUtil.disableBigQueryStrategy(session)
  }

  override def supportsSparkVersion(sparkVersion: String): Boolean = {
    sparkVersion.startsWith("3.1")
  }
}
