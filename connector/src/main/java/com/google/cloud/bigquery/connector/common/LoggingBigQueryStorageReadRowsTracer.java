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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

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

  LoggingBigQueryStorageReadRowsTracer(String streamName, int logIntervalPowerOf2) {
    this.streamName = streamName;
    this.logIntervalPowerOf2 = logIntervalPowerOf2;
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

  private static String format(DurationTimer durationTimer) {
    long samples = durationTimer.getSamples();
    if (samples == 0) {
      return "Not enough samples.";
    }
    Duration average = durationTimer.getAccumulatedTime().dividedBy(samples);
    return String.format("Average (ns): %s Samples: %d", average.toString(), samples);
  }

  private static long perSecond(DurationTimer timer, long metric) {
    if (timer.getSamples() == 0) {
      return 0;
    }
    Duration time = timer.getAccumulatedTime();
    double seconds =  (time.toMillis() / 1000.0);
    if (seconds != 0) {
      return (long) (metric / seconds);
    }
    return 0;
  }

  private void logData() {
    log.info(
        "{}: Started: {} Ended: {} Parse Timings: {}  Time in Spark: {} Time waiting for service: {} Rows/s {} Bytes/s {}",
        streamName,
        startTime,
        endTime == null ? "" : endTime.toString(),
        format(parseTime),
        format(sparkTime),
        format(serviceTime),
        perSecond(serviceTime, bytes),
        perSecond(parseTime, rows));
    linesLogged++;
  }

  @Override
  public void nextBatchNeeded() {
    sparkTime.finish();
    if (((sparkTime.getSamples() + 1) & ((1 << logIntervalPowerOf2) - 1)) == 0) {
      logData();
    }
  }
}
