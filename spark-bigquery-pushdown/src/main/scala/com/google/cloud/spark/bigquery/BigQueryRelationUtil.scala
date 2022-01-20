package com.google.cloud.spark.bigquery

import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import com.google.cloud.bigquery.connector.common.BigQueryProxyTransporterBuilder
import com.google.cloud.http.HttpTransportOptions
import org.apache.spark.internal.Logging

import java.util.Optional

object BigQueryRelationUtil extends Logging {

  def createBigQuery(options: SparkBigQueryConfig): BigQuery = {
    val credentials = options.createCredentials()
    val parentProjectId = options.getParentProjectId()
    logInfo(
      s"BigQuery client project id is [$parentProjectId], derived from the parentProject option")
    val bqOptions =
      BigQueryOptions.newBuilder()
        .setProjectId(parentProjectId)
        .setCredentials(credentials)
        .setRetrySettings(options.getBigQueryClientRetrySettings);

    val httpTransportOptionsBuilder =
      HttpTransportOptions.newBuilder
        .setConnectTimeout(options.getBigQueryClientConnectTimeout)
        .setReadTimeout(options.getBigQueryClientReadTimeout)
    val proxyHttpConfig = options.getBigQueryProxyConfig
    if (proxyHttpConfig.getProxyUri.isPresent) {
      httpTransportOptionsBuilder
        .setHttpTransportFactory(
          BigQueryProxyTransporterBuilder.createHttpTransportFactory(
            proxyHttpConfig.getProxyUri,
            proxyHttpConfig.getProxyUsername,
            proxyHttpConfig.getProxyPassword))
    }

    bqOptions.setTransportOptions(httpTransportOptionsBuilder.build)
    bqOptions.build().getService
  }

  def toOption[T](javaOptional: Optional[T]): Option[T] =
    if (javaOptional.isPresent) Some(javaOptional.get) else None

  def noneIfEmpty(s: String): Option[String] = Option(s).filterNot(_.trim.isEmpty)

}
