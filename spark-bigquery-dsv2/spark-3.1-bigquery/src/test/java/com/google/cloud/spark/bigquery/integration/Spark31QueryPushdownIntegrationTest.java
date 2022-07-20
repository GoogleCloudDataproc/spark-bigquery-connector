package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.pushdowns.BigQueryConnectorUtils;
import org.junit.Test;

public class Spark31QueryPushdownIntegrationTest extends QueryPushdownIntegrationTestBase {
  public Spark31QueryPushdownIntegrationTest() {
    BigQueryConnectorUtils.enablePushdownSession(spark);
  }

  @Test
  public void testSomething() {
    testBasicExpressions();
  }
}
