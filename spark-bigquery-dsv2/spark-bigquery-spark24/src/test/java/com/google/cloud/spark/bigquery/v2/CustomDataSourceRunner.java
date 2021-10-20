package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CustomDataSourceRunner {
  public static void main(String[] args) {
    SparkSession sparkSession = new CustomDataSourceRunner().getDefaultSparkSessionOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");
    Dataset<Row> simpleDf =
        sparkSession
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data:samples.shakespeare")
            .option("credentials", "/home/hdoop/tidy-tine-318906-56d63fd04176.json")
            .load();
    simpleDf.show();
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
