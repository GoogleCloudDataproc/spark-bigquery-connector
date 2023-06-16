package com.google.cloud.spark.bigquery.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.cloud.bigquery.connector.common.BigQueryMetrics;
import java.io.*;
import java.util.concurrent.TimeUnit;
import org.apache.spark.metrics.source.Source;

public class SparkMetricsSource implements Source, Externalizable, BigQueryMetrics {
  private transient MetricRegistry registry;
  private transient Counter bytesRead;
  private transient Counter rowsRead;
  private transient Timer scanTime;

  public SparkMetricsSource() {
    registry = new MetricRegistry();
    bytesRead = new Counter();
    rowsRead = new Counter();
    scanTime = new Timer();
    registry.register("bytesRead", bytesRead);
    registry.register("rowsRead", rowsRead);
    registry.register("scanTime", scanTime);
  }

  @Override
  public String sourceName() {
    return "bigquery-metrics-source";
  }

  @Override
  public MetricRegistry metricRegistry() {
    return registry;
  }

  @Override
  public void incrementBytesReadCounter(long val) {
    bytesRead.inc(val);
  }

  @Override
  public void incrementRowsReadCounter(long val) {
    rowsRead.inc(val);
  }

  @Override
  public void incrementScanTimeCounter(long val) {
    scanTime.update(val, TimeUnit.MILLISECONDS);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {}

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {}
}
