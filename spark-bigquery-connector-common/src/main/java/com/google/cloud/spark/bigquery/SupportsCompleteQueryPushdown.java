package com.google.cloud.spark.bigquery;

import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory;

public interface SupportsCompleteQueryPushdown {
  public BigQueryRDDFactory getBigQueryRDDFactory();
}
