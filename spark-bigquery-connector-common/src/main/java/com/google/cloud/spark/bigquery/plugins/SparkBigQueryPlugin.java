package com.google.cloud.spark.bigquery.plugins;

import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

public class SparkBigQueryPlugin implements SparkPlugin {

  @Override
  public DriverPlugin driverPlugin() {
    return new SparkBigQueryDriverPlugin();
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    return new SparkBigQueryExecutorPlugin();
  }
}
