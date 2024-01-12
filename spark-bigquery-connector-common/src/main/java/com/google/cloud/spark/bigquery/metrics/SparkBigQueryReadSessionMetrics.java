/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.metrics;

import com.google.cloud.bigquery.connector.common.ReadSessionMetrics;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.base.Objects;
import java.io.Serializable;
import java.lang.reflect.Method;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
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
  public final LongAccumulator rowsReadAccumulator;
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
    this.sessionId = sessionName;

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

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.sessionId,
        this.bytesReadAccumulator.id(),
        this.rowsReadAccumulator.id(),
        this.parseTimeAccumulator.id(),
        this.scanTimeAccumulator.id());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SparkBigQueryReadSessionMetrics) {
      SparkBigQueryReadSessionMetrics listenerObject = (SparkBigQueryReadSessionMetrics) o;
      return listenerObject.sessionId.equals(this.sessionId)
          && listenerObject.bytesReadAccumulator.id() == this.bytesReadAccumulator.id()
          && listenerObject.rowsReadAccumulator.id() == this.rowsReadAccumulator.id()
          && listenerObject.scanTimeAccumulator.id() == this.scanTimeAccumulator.id()
          && listenerObject.parseTimeAccumulator.id() == this.parseTimeAccumulator.id();
    }
    return false;
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

  public LongAccumulator getBytesReadAccumulator() {
    return bytesReadAccumulator;
  }

  public LongAccumulator getRowsReadAccumulator() {
    return rowsReadAccumulator;
  }

  public LongAccumulator getParseTimeAccumulator() {
    return parseTimeAccumulator;
  }

  public LongAccumulator getScanTimeAccumulator() {
    return scanTimeAccumulator;
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
      sparkSession.sparkContext().removeSparkListener(this);
    } catch (ReflectiveOperationException ignored) {
      logger.debug(
          "spark.events.BigQueryConnectorReadSessionMetricEvent library not in class path");
    }
  }
}
