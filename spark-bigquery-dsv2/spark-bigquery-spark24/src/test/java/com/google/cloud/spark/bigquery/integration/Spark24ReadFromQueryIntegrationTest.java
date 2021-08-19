package com.google.cloud.spark.bigquery.integration;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class Spark24ReadFromQueryIntegrationTest extends ReadFromQueryIntegrationTestBase {

  static IntegrationTestContext ctx;

  public Spark24ReadFromQueryIntegrationTest() {
    super(ctx);
  }

  @BeforeClass
  public static void initialize() {
    ctx = IntegrationTestUtils.initialize(Spark24ReadFromQueryIntegrationTest.class, true);
  }

  @AfterClass
  public static void clean() {

  }
}
