package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.BigQueryConnectorUtils;

public class DatasourceV1QueryPushdownIntegrationTest extends QueryPushdownIntegrationTestBase{

  public DatasourceV1QueryPushdownIntegrationTest() {
    BigQueryConnectorUtils.enablePushdownSession(spark);
  }
}
