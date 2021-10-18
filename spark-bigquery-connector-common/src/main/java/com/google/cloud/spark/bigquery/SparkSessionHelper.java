package com.google.cloud.spark.bigquery;

import org.apache.spark.sql.SparkSession;


public class SparkSessionHelper {
    public static SparkSession getDefaultSparkSessionOrCreate() {
        scala.Option<SparkSession> defaultSpareSession = SparkSession.getActiveSession();
        if (defaultSpareSession.isDefined()) {
            return defaultSpareSession.get();
        }
        return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
    }

}

