package com.google.cloud.spark.bigquery.integration;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class DataSourceV1WriteIntegrationTest extends WriteIntegrationTestBase {

  static IntegrationTestContext ctx;

  public DataSourceV1WriteIntegrationTest() {
    super(ctx);
  }

  @BeforeClass
  public static void initialize() {
    ctx = IntegrationTestUtils.initialize(DataSourceV1WriteIntegrationTest.class, false);
  }

  @AfterClass
  public static void clean() {
    IntegrationTestUtils.clean(ctx);
  }
}
