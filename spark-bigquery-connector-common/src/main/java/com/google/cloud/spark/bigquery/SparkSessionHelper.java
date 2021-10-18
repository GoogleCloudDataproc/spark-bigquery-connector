package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Optional;


public class SparkSessionHelper {
    // This method is used to create spark session
    public static SparkSession getDefaultSparkSessionOrCreate() {
        scala.Option<SparkSession> defaultSpareSession = SparkSession.getActiveSession();
        if (defaultSpareSession.isDefined()) {
            return defaultSpareSession.get();
        }
        return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
    }
    // This method is used to create injection by providing
    public static Injector createInjector(StructType schema, Map<String, String> options, Module module) {
        SparkSession spark = SparkSessionHelper.getDefaultSparkSessionOrCreate();
        return Guice.createInjector(
                new BigQueryClientModule(),
                new SparkBigQueryConnectorModule(
                        spark, options, Optional.ofNullable(schema), DataSourceVersion.V2),
                module);
    }

}

