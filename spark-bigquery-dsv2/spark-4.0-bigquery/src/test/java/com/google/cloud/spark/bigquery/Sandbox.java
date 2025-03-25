package com.google.cloud.spark.bigquery;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class Sandbox {

  @Test
  public void coltest() throws Exception {
    SparkSession spark = SparkSession.builder().master("local").appName("test").getOrCreate();
    spark.conf().set("viewsEnabled", "true");
    spark.conf().set("materializationDataset", "davidrab");
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .load("select col_a, col_b from davidrab.coltest order by col_a");
    df.show();
  }
}
