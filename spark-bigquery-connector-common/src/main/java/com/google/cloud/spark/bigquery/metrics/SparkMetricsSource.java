package com.google.cloud.spark.bigquery.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.cloud.bigquery.connector.common.BigQueryMetrics;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.spark.metrics.source.Source;

public class SparkMetricsSource implements Source, Serializable, BigQueryMetrics {
    private transient MetricRegistry registry;
    private transient Timer parseTime;
    private transient Timer timeInSpark;
    private transient Counter bytesRead;
    private transient Counter rowsRead;
    private transient Timer scanTime;

    private final String sessionName;

    public SparkMetricsSource(String sessionName) {
        registry = new MetricRegistry();
        parseTime = new Timer();
        timeInSpark = new Timer();
        bytesRead = new Counter();
        rowsRead = new Counter();
        scanTime = new Timer();
        registry.register("parseTime", parseTime);
        registry.register("timeInSpark", timeInSpark);
        registry.register("bytesRead", bytesRead);
        registry.register("rowsRead", rowsRead);
        registry.register("scanTime", scanTime);
        this.sessionName = sessionName;
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
    public void updateParseTime(long val) {
        parseTime.update(val, TimeUnit.MILLISECONDS);
        ReadSessionMetrics.forSession(sessionName).getParseTimeCounter().inc(val);
    }

    @Override
    public void updateTimeInSpark(long val) {
        timeInSpark.update(val, TimeUnit.MILLISECONDS);
    }

    @Override
    public void incrementBytesReadCounter(long val) {
        bytesRead.inc(val);
        ReadSessionMetrics.forSession(sessionName).getBytesReadCounter().inc(val);
    }

    @Override
    public void incrementRowsReadCounter(long val) {
        rowsRead.inc(val);
        ReadSessionMetrics.forSession(sessionName).getRowsReadCounter().inc(val);
    }

    @Override
    public void updateScanTime(long val) {
        scanTime.update(val, TimeUnit.MILLISECONDS);
        ReadSessionMetrics.forSession(sessionName).getScanTimeCounter().inc(val);
    }

    public Timer getParseTime() {
        return this.parseTime;
    }

    public Timer getTimeInSpark() {
        return this.timeInSpark;
    }

    public Timer getScanTime() {
        return this.scanTime;
    }

    public Counter getBytesRead() {
        return this.bytesRead;
    }

    public Counter getRowsRead() {
        return this.rowsRead;
    }
}
