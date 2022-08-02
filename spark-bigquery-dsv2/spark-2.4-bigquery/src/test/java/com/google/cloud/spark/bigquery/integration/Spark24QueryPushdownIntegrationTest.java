package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.pushdowns.BigQueryConnectorPushdown;

public class Spark24QueryPushdownIntegrationTest extends QueryPushdownIntegrationTestBase {
  public Spark24QueryPushdownIntegrationTest() {
    BigQueryConnectorPushdown.enablePushdownSession(spark);
  }
}
