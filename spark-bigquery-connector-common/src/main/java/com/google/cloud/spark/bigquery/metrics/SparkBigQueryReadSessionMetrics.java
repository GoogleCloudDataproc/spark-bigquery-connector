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
import com.google.cloud.bigquery.storage.v1.DataFormat;
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
  private static final String BYTES_READ = "bytesRead";
  private static final String ROWS_READ = "rowsRead";
  private static final String PARSE_TIME = "parseTime";
  private static final String SCAN_TIME = "scanTime";
  private static final long serialVersionUID = -8188694570364436143L;
  private final LongAccumulator bytesReadAccumulator;
  public final LongAccumulator rowsReadAccumulator;
  private final LongAccumulator scanTimeAccumulator;
  private final LongAccumulator parseTimeAccumulator;
  private final String sessionId;
  private final transient SparkSession sparkSession;
  private final long timestamp;
  private final DataFormat readDataFormat;
  private final DataOrigin dataOrigin;
  public long numReadStreams;

  private static final Logger logger =
      LoggerFactory.getLogger(SparkBigQueryReadSessionMetrics.class);

  private SparkBigQueryReadSessionMetrics(
      SparkSession sparkSession,
      String sessionName,
      long timestamp,
      DataFormat readDataFormat,
      DataOrigin dataOrigin,
      long numReadStreams) {
    this.sparkSession = sparkSession;
    this.sessionId = sessionName;
    this.timestamp = timestamp;
    this.readDataFormat = readDataFormat;
    this.dataOrigin = dataOrigin;
    this.numReadStreams = numReadStreams;

    this.bytesReadAccumulator =
        sparkSession
            .sparkContext()
            .longAccumulator(
                SparkBigQueryConnectorMetricsUtils.getAccumulatorNameForMetric(
                    BYTES_READ, sessionName));
    this.rowsReadAccumulator =
        sparkSession
            .sparkContext()
            .longAccumulator(
                SparkBigQueryConnectorMetricsUtils.getAccumulatorNameForMetric(
                    ROWS_READ, sessionName));
    this.scanTimeAccumulator =
        sparkSession
            .sparkContext()
            .longAccumulator(
                SparkBigQueryConnectorMetricsUtils.getAccumulatorNameForMetric(
                    SCAN_TIME, sessionName));
    this.parseTimeAccumulator =
        sparkSession
            .sparkContext()
            .longAccumulator(
                SparkBigQueryConnectorMetricsUtils.getAccumulatorNameForMetric(
                    PARSE_TIME, sessionName));
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
      SparkSession sparkSession,
      ReadSession readSession,
      long timestamp,
      com.google.cloud.bigquery.storage.v1.DataFormat readDataFormat,
      DataOrigin dataOrigin,
      long numReadStreams) {
    return new SparkBigQueryReadSessionMetrics(
        sparkSession, readSession.getName(), timestamp, readDataFormat, dataOrigin, numReadStreams);
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
              "com.google.cloud.spark.events.SparkBigQueryConnectorReadSessionEvent$SparkBigQueryConnectorReadSessionEventBuilder");

      Object builderInstance =
          eventBuilderClass.getDeclaredConstructor(long.class).newInstance(timestamp);
      eventBuilderClass
          .getMethod("setBytesRead", long.class)
          .invoke(builderInstance, getBytesRead());
      eventBuilderClass
          .getMethod("setNumberOfReadStreams", long.class)
          .invoke(builderInstance, numReadStreams);
      eventBuilderClass
          .getMethod("setParseTimeInMs", long.class)
          .invoke(builderInstance, getParseTime());
      eventBuilderClass
          .getMethod("setScanTimeInMs", long.class)
          .invoke(builderInstance, getScanTime());

      Class<?> dataFormatEnum = Class.forName("com.google.cloud.spark.events.DataFormat");
      Object[] dataFormatEnumConstants = dataFormatEnum.getEnumConstants();
      Object generatedDataFormatEnumValue = dataFormatEnumConstants[0];
      for (Object constant : dataFormatEnumConstants) {
        Method nameMethod = constant.getClass().getMethod("name");
        String name = (String) nameMethod.invoke(constant);
        if (name.equals(readDataFormat.toString())) {
          generatedDataFormatEnumValue = constant;
          break;
        }
      }
      eventBuilderClass
          .getMethod("setReadDataFormat", dataFormatEnum)
          .invoke(builderInstance, generatedDataFormatEnumValue);

      Class<?> dataOriginEnum = Class.forName("com.google.cloud.spark.events.DataOrigin");
      Object[] dataOriginEnumConstants = dataOriginEnum.getEnumConstants();
      Object generatedDataOriginEnumValue = dataOriginEnumConstants[0];
      for (Object constant : dataOriginEnumConstants) {
        Method nameMethod = constant.getClass().getMethod("name");
        String name = (String) nameMethod.invoke(constant);
        if (name.equals(dataOrigin.toString())) {
          generatedDataOriginEnumValue = constant;
          break;
        }
      }
      eventBuilderClass
          .getMethod("setDataOrigin", dataOriginEnum)
          .invoke(builderInstance, generatedDataOriginEnumValue);

      Method buildMethod = eventBuilderClass.getDeclaredMethod("build");

      sparkSession
          .sparkContext()
          .listenerBus()
          .post((SparkListenerEvent) buildMethod.invoke(builderInstance));

      sparkSession.sparkContext().removeSparkListener(this);
    } catch (ReflectiveOperationException ex) {
      logger.debug("spark.events.SparkBigQueryConnectorReadSessionEvent library not in class path");
    }
  }
}
