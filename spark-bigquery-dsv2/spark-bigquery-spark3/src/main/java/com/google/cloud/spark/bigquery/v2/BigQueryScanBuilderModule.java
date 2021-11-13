package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.spark.sql.SparkSession;

public class BigQueryScanBuilderModule implements Module {
  @Override
  public void configure(Binder binder) {}

  @Singleton
  @Provides
  public BigQueryScanBuilder provideScanbuilder(
      BigQueryClient bigQueryClient,
      BigQueryReadClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      SparkBigQueryConfig config,
      SparkSession sparkSession) {
    TableInfo tableInfo = bigQueryClient.getReadTable(config.toReadTableOptions());
    return new BigQueryScanBuilder(
        tableInfo,
        bigQueryClient,
        bigQueryReadClientFactory,
        tracerFactory,
        config.toReadSessionCreatorConfig(),
        config.getFilter(),
        config.getSchema(),
        sparkSession.sparkContext().applicationId());
  }
}
