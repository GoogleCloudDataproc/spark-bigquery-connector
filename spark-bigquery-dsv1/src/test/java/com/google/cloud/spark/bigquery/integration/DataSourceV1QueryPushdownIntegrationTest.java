package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.BigQueryConnectorUtils;

public class DataSourceV1QueryPushdownIntegrationTest extends QueryPushdownIntegrationTestBase {

  public DataSourceV1QueryPushdownIntegrationTest() {
    BigQueryConnectorUtils.enablePushdownSession(spark);
  }
}
