package com.google.cloud.spark.bigquery

import org.apache.spark.sql.SparkSession

object SparkBigQueryPushdownUtil {
  def enablePushdownSession(session: SparkSession): Unit = {
    throw new NotImplementedError("Query Pushdown is not implemented for Scala 2.11")
  }

  def disablePushdownSession(session: SparkSession): Unit = {
    throw new NotImplementedError("Query Pushdown is not implemented for Scala 2.11")
  }
}
