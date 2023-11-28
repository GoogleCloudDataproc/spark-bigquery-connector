package com.google.cloud.spark.bigquery.metrics;

import com.google.cloud.bigquery.connector.common.ReadSessionMetrics;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import java.io.Serializable;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger logger =
      LoggerFactory.getLogger(SparkBigQueryReadSessionMetrics.class);

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
    logger.info("bytesReadAccumulator.id() = " + bytesReadAccumulator.id());

    this.rowsReadAccumulator =
        sparkSession
            .sparkContext()
            .longAccumulator(
                SparkBigQueryConnectorMetricsUtils.getAccumulatorNameForMetric(
                    rowsRead, sessionName));

    logger.info("rowsReadAccumulator.id() = " + rowsReadAccumulator.id());
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
    logger.info("Increment : bytesReadAccumulator value = " + bytesReadAccumulator.value());
  }

  public void incrementRowsReadAccumulator(long value) {
    rowsReadAccumulator.add(value);
    logger.info("Increment : rowsReadAccumulator value = " + rowsReadAccumulator.value());
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
  public void onApplicationEnd(SparkListenerApplicationEnd jobEnd) {
    logger.info("bytesReadAccumulator value = " + bytesReadAccumulator.value());
    logger.info("rowsReadAccumulator value = " + rowsReadAccumulator.value());
    //    try {
    //      Class<?> eventBuilderClass =
    //          Class.forName(
    //
    // "com.google.cloud.spark.events.BigQueryConnectorReadSessionMetricEvent$BigQueryConnectorReadSessionMetricEventBuilder");
    //
    //      Method buildMethod = eventBuilderClass.getDeclaredMethod("build");
    //
    //      sparkSession
    //          .sparkContext()
    //          .listenerBus()
    //          .post(
    //              (SparkListenerEvent)
    //                  buildMethod.invoke(
    //                      eventBuilderClass
    //                          .getDeclaredConstructor(String.class, String.class, Long.class)
    //                          .newInstance(sessionId, readStreams, numReadStreams)));
    //
    //      sparkSession
    //          .sparkContext()
    //          .listenerBus()
    //          .post(
    //              (SparkListenerEvent)
    //                  buildMethod.invoke(
    //                      eventBuilderClass
    //                          .getDeclaredConstructor(String.class, String.class, Long.class)
    //                          .newInstance(sessionId, bytesRead, getBytesRead())));
    //
    //      sparkSession
    //          .sparkContext()
    //          .listenerBus()
    //          .post(
    //              (SparkListenerEvent)
    //                  buildMethod.invoke(
    //                      eventBuilderClass
    //                          .getDeclaredConstructor(String.class, String.class, Long.class)
    //                          .newInstance(sessionId, rowsRead, getRowsRead())));
    //
    //      sparkSession
    //          .sparkContext()
    //          .listenerBus()
    //          .post(
    //              (SparkListenerEvent)
    //                  buildMethod.invoke(
    //                      eventBuilderClass
    //                          .getDeclaredConstructor(String.class, String.class, Long.class)
    //                          .newInstance(sessionId, parseTime, getParseTime())));
    //
    //      sparkSession
    //          .sparkContext()
    //          .listenerBus()
    //          .post(
    //              (SparkListenerEvent)
    //                  buildMethod.invoke(
    //                      eventBuilderClass
    //                          .getDeclaredConstructor(String.class, String.class, Long.class)
    //                          .newInstance(sessionId, scanTime, getScanTime())));
    //
    //    } catch (ReflectiveOperationException ignored) {
    //    }
  }
}
