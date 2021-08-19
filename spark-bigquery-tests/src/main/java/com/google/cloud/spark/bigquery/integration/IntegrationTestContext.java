package com.google.cloud.spark.bigquery.integration;

import org.apache.spark.sql.SparkSession;

public class IntegrationTestContext {

  private SparkSession spark;
  private String testDataset;

  IntegrationTestContext(SparkSession spark, String testDataset) {
    this.spark = spark;
    this.testDataset = testDataset;
  }

  public SparkSession getSpark() {
    return spark;
  }

  public String getTestDataset() {
    return testDataset;
  }
}

