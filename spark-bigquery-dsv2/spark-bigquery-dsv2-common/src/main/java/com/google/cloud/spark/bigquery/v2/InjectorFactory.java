package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SparkBigQueryConnectorModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Optional;

public class InjectorFactory {
    private InjectorFactory() {}

    public static Injector createInjector(StructType schema, Map<String, String> options, Module module) {
        SparkSession spark = getDefaultSparkSessionOrCreate();
        return Guice.createInjector(
                new BigQueryClientModule(),
                new SparkBigQueryConnectorModule(
                        spark, options, Optional.ofNullable(schema), DataSourceVersion.V2),
                module);
    }

    protected static SparkSession getDefaultSparkSessionOrCreate() {
        scala.Option<SparkSession> defaultSpareSession = SparkSession.getActiveSession();
        if (defaultSpareSession.isDefined()) {
            return defaultSpareSession.get();
        }
        return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
    }

}
