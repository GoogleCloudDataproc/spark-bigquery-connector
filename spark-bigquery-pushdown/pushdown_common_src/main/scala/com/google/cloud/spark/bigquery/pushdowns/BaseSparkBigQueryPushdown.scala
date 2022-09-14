package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.SparkSession

abstract class BaseSparkBigQueryPushdown extends SparkBigQueryPushdown {

  def supportsSparkVersion(sparkVersion: String): Boolean

  override def enable(session: SparkSession): Unit = {
    val sparkExpressionFactory = createSparkExpressionFactory
    val sparkPlanFactory = createSparkPlanFactory
    val sparkExpressionConverter = createSparkExpressionConverter(
      sparkExpressionFactory, sparkPlanFactory)
    val bigQueryStrategy = createBigQueryStrategy(
      sparkExpressionConverter, sparkExpressionFactory, sparkPlanFactory)
    SparkBigQueryPushdownUtil.enableBigQueryStrategy(session, bigQueryStrategy)
  }

  override def disable(session: SparkSession): Unit = {
    SparkBigQueryPushdownUtil.disableBigQueryStrategy(session)
  }

  def createSparkExpressionConverter(expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory): SparkExpressionConverter

  def createSparkExpressionFactory: SparkExpressionFactory

  def createSparkPlanFactory(): SparkPlanFactory

  def createBigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory): BigQueryStrategy
}
