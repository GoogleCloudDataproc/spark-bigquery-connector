/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link BigQueryStorageReadRowsTracer} that accumulates and logs times
 * periodically.
 */
public class LoggingBigQueryStorageReadRowsTracer implements BigQueryStorageReadRowsTracer {
  private static final Logger log =
      LoggerFactory.getLogger(LoggingBigQueryStorageReadRowsTracer.class);

  private final String streamName;
  private final int logIntervalPowerOf2;
  // Visible for testing.
  Instant startTime;
  final DurationTimer parseTime = new DurationTimer();
  final DurationTimer sparkTime = new DurationTimer();
  final DurationTimer serviceTime = new DurationTimer();
  Instant endTime;
  long rows = 0;
  long bytes = 0;
  // For confirming data is logged.
  long linesLogged = 0;
  BigQueryMetrics bigQueryMetrics;

  Optional<ReadSessionMetrics> readSessionMetrics;

  LoggingBigQueryStorageReadRowsTracer(
      String streamName,
      int logIntervalPowerOf2,
      BigQueryMetrics bigQueryMetrics,
      Optional<ReadSessionMetrics> readSessionMetrics) {
    this.streamName = streamName;
    this.logIntervalPowerOf2 = logIntervalPowerOf2;
    this.bigQueryMetrics = bigQueryMetrics;
    this.readSessionMetrics = readSessionMetrics;
  }

  @Override
  public void startStream() {
    startTime = Instant.now();
  }

  @Override
  public void rowsParseStarted() {
    parseTime.start();
  }

  @Override
  public void rowsParseFinished(long rows) {
    this.rows += rows;
    parseTime.finish();
  }

  @Override
  public void readRowsResponseRequested() {
    serviceTime.start();
  }

  @Override
  public void readRowsResponseObtained(long bytes) {
    this.bytes += bytes;
    serviceTime.finish();
  }

  @Override
  public void finished() {
    endTime = Instant.now();
    logData();
  }

  private static Duration average(DurationTimer durationTimer) {
    long samples = durationTimer.getSamples();
    if (samples == 0) {
      return null;
    }
    return durationTimer.getAccumulatedTime().dividedBy(samples);
  }

  private static String format(DurationTimer durationTimer) {
    long samples = durationTimer.getSamples();
    if (samples == 0) {
      return "Not enough samples.";
    }
    Duration average = average(durationTimer);
    return String.format("Average: %s Samples: %d", average.toString(), samples);
  }

  private static String difference(DurationTimer d1, DurationTimer d2) {
    if (d1.getSamples() == 0 || d2.getSamples() == 0) {
      return "Not enough samples.";
    }
    return String.format("Average: %s", average(d1).minus(average(d2)).toString());
  }

  private static long perSecond(DurationTimer timer, long metric) {
    if (timer.getSamples() == 0) {
      return 0;
    }
    Duration time = timer.getAccumulatedTime();
    double seconds = (time.toMillis() / 1000.0);
    if (seconds != 0) {
      return (long) (metric / seconds);
    }
    return 0;
  }

  private void logData() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("Stream Name", streamName);
    jsonObject.addProperty("Started", startTime == null ? "" : startTime.toString());
    jsonObject.addProperty("Ended", endTime == null ? "" : endTime.toString());
    jsonObject.addProperty("Parse Timings", format(parseTime));
    jsonObject.addProperty("Time in Spark", difference(sparkTime, parseTime));
    jsonObject.addProperty("Time waiting for service", format(serviceTime));
    jsonObject.addProperty("Bytes/s", perSecond(serviceTime, getBytesRead()));
    jsonObject.addProperty("Rows/s", perSecond(parseTime, getRowsRead()));
    jsonObject.addProperty("Bytes", getBytesRead());
    jsonObject.addProperty("Rows", getRowsRead());
    jsonObject.addProperty("I/O time in ms", getScanTimeInMilliSec());
    log.info("ReadStream Metrics :{}", new Gson().toJson(jsonObject));
    bigQueryMetrics.incrementBytesReadCounter(getBytesRead());
    bigQueryMetrics.incrementRowsReadCounter(getRowsRead());
    bigQueryMetrics.updateScanTime(getScanTimeInMilliSec());
    bigQueryMetrics.updateParseTime(getParseTimeInMilliSec());
    bigQueryMetrics.updateTimeInSpark(getTimeInSparkInMilliSec());
    readSessionMetrics.ifPresent(
        metrics -> {
          metrics.incrementBytesReadAccumulator(getBytesRead());
          metrics.incrementRowsReadAccumulator(getRowsRead());
          metrics.incrementParseTimeAccumulator(getParseTimeInMilliSec());
          metrics.incrementScanTimeAccumulator(getScanTimeInMilliSec());
        });
    linesLogged++;
  }

  @Override
  public void nextBatchNeeded() {
    sparkTime.finish();
    if (((sparkTime.getSamples() + 1) & ((1 << logIntervalPowerOf2) - 1)) == 0) {
      logData();
    }
  }

  @Override
  public BigQueryStorageReadRowsTracer forkWithPrefix(String id) {
    return new LoggingBigQueryStorageReadRowsTracer(
        "id-" + id + "-" + streamName, logIntervalPowerOf2, bigQueryMetrics, readSessionMetrics);
  }

  @Override
  public long getBytesRead() {
    return bytes;
  }

  @Override
  public long getRowsRead() {
    return rows;
  }

  @Override
  public long getScanTimeInMilliSec() {
    return serviceTime.getAccumulatedTime().toMillis();
  }

  @Override
  public long getParseTimeInMilliSec() {
    return parseTime.getAccumulatedTime().toMillis();
  }

  @Override
  public long getTimeInSparkInMilliSec() {
    return sparkTime.getAccumulatedTime().minus(parseTime.getAccumulatedTime()).toMillis();
  }

  String getStreamName() {
    return streamName;
  }
}
