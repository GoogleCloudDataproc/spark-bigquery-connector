package com.google.cloud.spark.bigquery.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.cloud.bigquery.connector.common.BigQueryMetrics;
import java.io.*;
import org.apache.spark.metrics.source.Source;

public class SparkMetricsSource implements Source, Serializable, BigQueryMetrics {
  private transient MetricRegistry registry;
  private transient Counter bytesReadCounter;

  public SparkMetricsSource() {
    registry = new MetricRegistry();
    bytesReadCounter = new Counter();
    registry.register("bytesRead", bytesReadCounter);
  }

  @Override
  public String sourceName() {
    return "bigquery-metrics-source";
  }

  @Override
  public MetricRegistry metricRegistry() {
    return registry;
  }

  public void incrementBytesReadCounter(long value) {
    bytesReadCounter.inc(value);
  }

  @Override
  public void setBytesRead(long val) {
    incrementBytesReadCounter(val);
  }
}
