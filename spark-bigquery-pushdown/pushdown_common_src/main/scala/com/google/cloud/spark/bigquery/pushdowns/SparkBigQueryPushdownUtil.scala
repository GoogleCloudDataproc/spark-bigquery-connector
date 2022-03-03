package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.SparkSession

object SparkBigQueryPushdownUtil {
  def enableBigQueryStrategy(session: SparkSession, bigQueryStrategy: BigQueryStrategy): Unit = {
    if (!session.experimental.extraStrategies.exists(
      s => s.isInstanceOf[BigQueryStrategy]
    )) {
      session.experimental.extraStrategies ++= Seq(bigQueryStrategy)
    }
  }

  def disableBigQueryStrategy(session: SparkSession): Unit = {
    session.experimental.extraStrategies = session.experimental.extraStrategies
      .filterNot(strategy => strategy.isInstanceOf[BigQueryStrategy])
  }
}
