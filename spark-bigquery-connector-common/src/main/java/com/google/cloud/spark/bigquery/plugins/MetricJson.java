package com.google.cloud.spark.bigquery.plugins;

import java.io.Serializable;

public class MetricJson implements Serializable {
  public String metricName;
  public Long metricValue;
  public String executorID;

  public MetricJson(String executorID, String metricName, Long metricValue) {
    this.executorID = executorID;
    this.metricName = metricName;
    this.metricValue = metricValue;
  }

  public Long getMetricValue() {
    return metricValue;
  }

  public String getMetricName() {
    return metricName;
  }
}
