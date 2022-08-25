package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.BigQueryConnectorUtils;

public class Spark31QueryPushdownIntegrationTest extends QueryPushdownIntegrationTestBase {
  public Spark31QueryPushdownIntegrationTest() {
    BigQueryConnectorUtils.enablePushdownSession(spark);
  }
}
