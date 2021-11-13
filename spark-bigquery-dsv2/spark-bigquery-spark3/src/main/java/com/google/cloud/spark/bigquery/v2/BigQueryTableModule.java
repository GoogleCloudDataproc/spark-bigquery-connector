package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDataSourceWriterModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.Map;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class BigQueryTableModule implements Module {
  private Map<String, String> properties;
  private StructType schema;
  private GenericBigQueryDataSourceWriterModule dataSourceWriterModuleHelper;
  private BigQueryClient bigQueryClient;

  @Override
  public void configure(Binder binder) {
    // empty
  }

  public BigQueryTableModule(StructType schema, Map<String, String> properties) {
    this.schema = schema;
    this.properties = properties;
  }

  @Singleton
  @Provides
  public BigQueryTable provideDataSource(
      BigQueryClient bigQueryClient,
      BigQueryReadClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      SparkBigQueryConfig config,
      SparkSession sparkSession)
      throws AnalysisException {
    return new BigQueryTable(
        config.getSchema().get(), null, this.properties, bigQueryClient, config, sparkSession);
  }
}
