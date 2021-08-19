package com.google.cloud.spark.bigquery.integration;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class DataSourceV1ReadFromQueryIntegrationTest extends ReadFromQueryIntegrationTestBase {

  static IntegrationTestContext ctx;

  public DataSourceV1ReadFromQueryIntegrationTest() {
    super(ctx);
  }

  @BeforeClass
  public static void initialize() {
    ctx = IntegrationTestUtils.initialize(DataSourceV1ReadFromQueryIntegrationTest.class, true);
  }

  @AfterClass
  public static void clean() {
    IntegrationTestUtils.clean(ctx);
  }
}
