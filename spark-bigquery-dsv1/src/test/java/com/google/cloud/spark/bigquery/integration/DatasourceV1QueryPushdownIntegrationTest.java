package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.pushdowns.BigQueryConnectorUtils;
import org.junit.Ignore;

@Ignore // not working yet
public class DatasourceV1QueryPushdownIntegrationTest extends QueryPushdownIntegrationTestBase {

  public DatasourceV1QueryPushdownIntegrationTest() {
    BigQueryConnectorUtils.enablePushdownSession(spark);
  }
}
