package com.google.cloud.spark.bigquery

import com.google.cloud.spark.bigquery.pushdowns.BigQueryPushdown
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

import java.util.ServiceLoader

object BigQueryConnectorUtils {
  def enablePushdownSession(session: SparkSession): Unit = {
    // Find the supported implementation based on Spark version and enable pushdown
    val bigQueryPushdownService = ServiceLoader.load(classOf[BigQueryPushdown])
      .iterator().asScala.find(p => p.supportsSparkVersion(session.version))
      .getOrElse(sys.error(s"Query pushdown not supported for Spark version ${session.version}"))
    // TODO: pass BigQueryStrategy with any required dependencies injected
    bigQueryPushdownService.enable(session)
  }

  def disablePushdownSession(session: SparkSession): Unit = {
    // Find the supported implementation based on Spark version and disable pushdown
    val bigQueryPushdownService = ServiceLoader.load(classOf[BigQueryPushdown])
      .iterator().asScala.find(p => p.supportsSparkVersion(session.version))
      .getOrElse(sys.error(s"Query pushdown not supported for Spark version ${session.version}"))
    bigQueryPushdownService.disable(session)
  }
}
