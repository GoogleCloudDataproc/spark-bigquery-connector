package com.google.cloud.spark.bigquery.metrics;

import com.google.cloud.bigquery.connector.common.ReadSessionMetrics;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;
import java.lang.reflect.Method;

public class SparkBigQueryReadSessionMetrics extends SparkListener
    implements Serializable, ReadSessionMetrics {
  private static final String bytesRead = "bqBytesRead";
  private static final String rowsRead = "bqRowsRead";
  private static final String scanTime = "bqScanTime";
  private static final String parseTime = "bqParseTime";
  private static final String readStreams = "bqReadStreams";
  private final LongAccumulator bytesReadAccumulator;
  private final LongAccumulator rowsReadAccumulator;
  private final LongAccumulator scanTimeAccumulator;
  private final LongAccumulator parseTimeAccumulator;
  public long numReadStreams;

  private final String sessionId;
  private final SparkSession sparkSession;

  private SparkBigQueryReadSessionMetrics(
      SparkSession sparkSession, String sessionName, long numReadStreams) {
    this.numReadStreams = numReadStreams;
    this.sparkSession = sparkSession;
    this.sessionId =
        SparkBigQueryConnectorMetricsUtils.extractDecodedSessionIdFromSessionName(sessionName);

    this.bytesReadAccumulator =
        sparkSession
            .sparkContext()
            .longAccumulator(
                SparkBigQueryConnectorMetricsUtils.getAccumulatorNameForMetric(
                    bytesRead, sessionName));

    this.rowsReadAccumulator =
        sparkSession
            .sparkContext()
            .longAccumulator(
                SparkBigQueryConnectorMetricsUtils.getAccumulatorNameForMetric(
                    rowsRead, sessionName));
    this.scanTimeAccumulator =
        sparkSession
            .sparkContext()
            .longAccumulator(
                SparkBigQueryConnectorMetricsUtils.getAccumulatorNameForMetric(
                    scanTime, sessionName));
    this.parseTimeAccumulator =
        sparkSession
            .sparkContext()
            .longAccumulator(
                SparkBigQueryConnectorMetricsUtils.getAccumulatorNameForMetric(
                    parseTime, sessionName));
  }

  public static SparkBigQueryReadSessionMetrics from(
      SparkSession sparkSession, ReadSession readSession, long numReadStreams) {
    return new SparkBigQueryReadSessionMetrics(sparkSession, readSession.getName(), numReadStreams);
  }

  public void incrementBytesReadAccumulator(long value) {
    bytesReadAccumulator.add(value);
  }

  public void incrementRowsReadAccumulator(long value) {
    rowsReadAccumulator.add(value);
  }

  public void incrementScanTimeAccumulator(long value) {
    scanTimeAccumulator.add(value);
  }

  public void incrementParseTimeAccumulator(long value) {
    parseTimeAccumulator.add(value);
  }

  public long getBytesRead() {
    return bytesReadAccumulator.value();
  }

  public long getRowsRead() {
    return rowsReadAccumulator.value();
  }

  public long getScanTime() {
    return scanTimeAccumulator.value();
  }

  public long getParseTime() {
    return parseTimeAccumulator.value();
  }

  public long getNumReadStreams() {
    return numReadStreams;
  }

  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    try {
      Class<?> eventBuilderClass =
          Class.forName(
              "com.google.cloud.spark.events.BigQueryConnectorReadSessionMetricEvent$BigQueryConnectorReadSessionMetricEventBuilder");

      Method buildMethod = eventBuilderClass.getDeclaredMethod("build");

      sparkSession
          .sparkContext()
          .listenerBus()
          .post(
              (SparkListenerEvent)
                  buildMethod.invoke(
                      eventBuilderClass
                          .getDeclaredConstructor(String.class, String.class, Long.class)
                          .newInstance(sessionId, readStreams, numReadStreams)));

      sparkSession
          .sparkContext()
          .listenerBus()
          .post(
              (SparkListenerEvent)
                  buildMethod.invoke(
                      eventBuilderClass
                          .getDeclaredConstructor(String.class, String.class, Long.class)
                          .newInstance(sessionId, bytesRead, getBytesRead())));

      sparkSession
          .sparkContext()
          .listenerBus()
          .post(
              (SparkListenerEvent)
                  buildMethod.invoke(
                      eventBuilderClass
                          .getDeclaredConstructor(String.class, String.class, Long.class)
                          .newInstance(sessionId, rowsRead, getRowsRead())));

      sparkSession
          .sparkContext()
          .listenerBus()
          .post(
              (SparkListenerEvent)
                  buildMethod.invoke(
                      eventBuilderClass
                          .getDeclaredConstructor(String.class, String.class, Long.class)
                          .newInstance(sessionId, parseTime, getParseTime())));

      sparkSession
          .sparkContext()
          .listenerBus()
          .post(
              (SparkListenerEvent)
                  buildMethod.invoke(
                      eventBuilderClass
                          .getDeclaredConstructor(String.class, String.class, Long.class)
                          .newInstance(sessionId, scanTime, getScanTime())));

    } catch (ReflectiveOperationException ignored) {
    }
  }
}
