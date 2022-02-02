package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SparkBigQueryConnectorModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class InjectorFactory {
  private InjectorFactory() {}

  public static Injector createInjector(StructType schema, Map<String, String> options) {
    SparkSession spark = SparkSession.active();
    return Guice.createInjector(
        new BigQueryClientModule(),
        new SparkBigQueryConnectorModule(
            spark, options, Optional.ofNullable(schema), DataSourceVersion.V2));
  }
}
