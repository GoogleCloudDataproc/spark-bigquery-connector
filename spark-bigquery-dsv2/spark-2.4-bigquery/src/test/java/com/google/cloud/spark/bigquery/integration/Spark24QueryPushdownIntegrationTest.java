package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.pushdowns.BigQueryConnectorUtils;
import org.junit.Test;

public class Spark24QueryPushdownIntegrationTest extends QueryPushdownIntegrationTestBase {
  public Spark24QueryPushdownIntegrationTest() {
    BigQueryConnectorUtils.enablePushdownSession(spark);
  }

  @Test
  public void testSomething() {
    testPushdownInnerJoin2();
  }
}
