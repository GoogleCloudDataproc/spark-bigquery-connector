package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.BigQueryConnectorUtils;
import org.junit.Before;
import org.junit.Test;

public class DataSourceV1QueryPushdownIntegrationTest extends QueryPushdownIntegrationTestBase {

  @Before
  public void before() {
    BigQueryConnectorUtils.enablePushdownSession(spark);
  }

  @Test
  public void testSomething() {
    testStringFunctionExpressions();
  }
}
