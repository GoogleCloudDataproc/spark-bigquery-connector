package com.google.cloud.spark.bigquery

import com.google.cloud.spark.bigquery.pushdowns.{BigQueryStrategy, SparkBigQueryPushdown}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import java.util.ServiceLoader

object BigQueryConnectorUtils {
  def enablePushdownSession(session: SparkSession): Unit = {
    // Find the supported implementation based on Spark version and enable pushdown
    val sparkBigQueryPushdown = ServiceLoader.load(classOf[SparkBigQueryPushdown])
      .iterator().asScala.find(p => p.supportsSparkVersion(session.version))
      .getOrElse(sys.error(s"Query pushdown not supported for Spark version ${session.version}"))
    // TODO: Inject dependencies in BigQueryStrategy
    sparkBigQueryPushdown.enable(session, new BigQueryStrategy)
  }

  def disablePushdownSession(session: SparkSession): Unit = {
    // Find the supported implementation based on Spark version and disable pushdown
    val sparkBigQueryPushdown = ServiceLoader.load(classOf[SparkBigQueryPushdown])
      .iterator().asScala.find(p => p.supportsSparkVersion(session.version))
      .getOrElse(sys.error(s"Query pushdown not supported for Spark version ${session.version}"))
    sparkBigQueryPushdown.disable(session)
  }
}
