package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.pushdowns.BigQueryConnectorPushdown;

public class Spark31QueryPushdownIntegrationTest extends QueryPushdownIntegrationTestBase {
  public Spark31QueryPushdownIntegrationTest() {
    BigQueryConnectorPushdown.enablePushdownSession(spark);
  }
}
