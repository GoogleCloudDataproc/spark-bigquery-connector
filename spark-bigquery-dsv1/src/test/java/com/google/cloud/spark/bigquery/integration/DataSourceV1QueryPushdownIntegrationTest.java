package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.pushdowns.BigQueryConnectorPushdown;

public class DataSourceV1QueryPushdownIntegrationTest extends QueryPushdownIntegrationTestBase {

  public DataSourceV1QueryPushdownIntegrationTest() {
    BigQueryConnectorPushdown.enablePushdownSession(spark);
  }
}
