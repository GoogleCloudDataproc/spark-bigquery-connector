package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CustomDataSourceRunner {
  public static void main(String[] args) {
   try {
     SparkSession sparkSession = new CustomDataSourceRunner().getDefaultSparkSessionOrCreate();
     Dataset<Row> simpleDf =
             sparkSession
                     .read()
                     .format("com.google.cloud.spark.bigquery.v2.BQTableProvider")
                     .load();
     simpleDf.show();

   }catch (Exception e)
   {
     e.printStackTrace();
   }
  }

  private SparkSession getDefaultSparkSessionOrCreate() {
    scala.Option<SparkSession> defaultSparkSession = SparkSession.getActiveSession();
    if (defaultSparkSession.isDefined()) {
      return defaultSparkSession.get();
    }
    return SparkSession.builder()
        .appName("spark-bigquery-connector")
        .master("local[*]")
        .getOrCreate();
  }
}
