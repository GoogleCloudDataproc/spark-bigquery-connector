package com.google.cloud.spark.bigquery

import com.google.cloud.spark.bigquery.pushdowns.BigQueryStrategy
import org.apache.spark.sql.SparkSession

object SparkBigQueryPushdownUtil {
  def enablePushdownSession(session: SparkSession): Unit = {
    if (!session.experimental.extraStrategies.exists(
      s => s.isInstanceOf[BigQueryStrategy]
    )) {
      session.experimental.extraStrategies ++= Seq(new BigQueryStrategy)
    }
  }

  def disablePushdownSession(session: SparkSession): Unit = {
    session.experimental.extraStrategies = session.experimental.extraStrategies
      .filterNot(strategy => strategy.isInstanceOf[BigQueryStrategy])
  }
}
