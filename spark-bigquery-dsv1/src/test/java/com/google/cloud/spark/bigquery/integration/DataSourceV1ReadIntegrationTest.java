package com.google.cloud.spark.bigquery.integration;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class DataSourceV1ReadIntegrationTest extends ReadIntegrationTestBase {

  static IntegrationTestContext ctx;

  public DataSourceV1ReadIntegrationTest() {
    super(ctx);
  }

  @BeforeClass
  public static void initialize() {
    ctx = IntegrationTestUtils.initialize(DataSourceV1ReadIntegrationTest.class, true);
  }

  @AfterClass
  public static void clean() {
    IntegrationTestUtils.clean(ctx);
  }
}
