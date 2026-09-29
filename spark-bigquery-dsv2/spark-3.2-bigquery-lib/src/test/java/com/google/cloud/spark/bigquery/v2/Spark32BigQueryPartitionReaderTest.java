/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.spark.bigquery.v2;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.spark.bigquery.v2.context.InputPartitionContext;
import com.google.cloud.spark.bigquery.v2.context.InputPartitionReaderContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.junit.Test;

public class Spark32BigQueryPartitionReaderTest {

  @Test
  public void currentMetricsValuesWithoutTracerContainsEstimate() {
    CustomTaskMetric[] metrics = currentMetricsValues(Optional.empty(), OptionalLong.of(1024L));

    assertThat(metrics).hasLength(1);
    assertThat(metrics[0].name()).isEqualTo("bqEstimatedBytesScanned");
    assertThat(metrics[0].value()).isEqualTo(1024L);
  }

  @Test
  public void currentMetricsValuesWithTracerContainsExistingMetricsAndEstimate() {
    CustomTaskMetric[] metrics =
        currentMetricsValues(Optional.of(new TestTracer()), OptionalLong.of(1024L));

    assertThat(metricsByName(metrics))
        .containsExactly(
            "bqBytesRead", 11L,
            "bqRowsRead", 12L,
            "bqScanTime", 13L,
            "bqParseTime", 14L,
            "bqTimeInSpark", 15L,
            "bqNumReadStreams", 1L,
            "bqEstimatedBytesScanned", 1024L);
  }

  @Test
  public void currentMetricsValuesWithoutReadSessionDoesNotContainEstimate() {
    assertThat(currentMetricsValues(Optional.empty(), OptionalLong.empty())).isEmpty();
  }

  private static CustomTaskMetric[] currentMetricsValues(
      Optional<BigQueryStorageReadRowsTracer> tracer, OptionalLong estimatedBytesScanned) {
    TestPartitionReaderContext readerContext = new TestPartitionReaderContext(tracer);
    TestInputPartitionContext partitionContext =
        new TestInputPartitionContext(readerContext, estimatedBytesScanned);
    PartitionReader<InternalRow> reader =
        new Spark32BigQueryPartitionReaderFactory()
            .createReader(new BigQueryInputPartition(partitionContext));
    return ((Spark32BigQueryPartitionReader<?>) reader).currentMetricsValues();
  }

  private static Map<String, Long> metricsByName(CustomTaskMetric[] metrics) {
    return Arrays.stream(metrics)
        .collect(Collectors.toMap(CustomTaskMetric::name, CustomTaskMetric::value));
  }

  private static class TestInputPartitionContext implements InputPartitionContext<InternalRow> {
    private final TestPartitionReaderContext readerContext;
    private final OptionalLong estimatedBytesScanned;

    TestInputPartitionContext(
        TestPartitionReaderContext readerContext, OptionalLong estimatedBytesScanned) {
      this.readerContext = readerContext;
      this.estimatedBytesScanned = estimatedBytesScanned;
    }

    @Override
    public InputPartitionReaderContext<InternalRow> createPartitionReaderContext() {
      return readerContext;
    }

    @Override
    public boolean supportColumnarReads() {
      return false;
    }

    @Override
    public OptionalLong getEstimatedBytesScanned() {
      return estimatedBytesScanned;
    }
  }

  private static class TestPartitionReaderContext
      implements InputPartitionReaderContext<InternalRow> {
    private final Optional<BigQueryStorageReadRowsTracer> tracer;

    TestPartitionReaderContext(Optional<BigQueryStorageReadRowsTracer> tracer) {
      this.tracer = tracer;
    }

    @Override
    public boolean next() {
      return false;
    }

    @Override
    public InternalRow get() {
      return null;
    }

    @Override
    public Optional<BigQueryStorageReadRowsTracer> getBigQueryStorageReadRowsTracer() {
      return tracer;
    }

    @Override
    public void close() throws IOException {}
  }

  private static class TestTracer implements BigQueryStorageReadRowsTracer {

    @Override
    public void startStream() {}

    @Override
    public void rowsParseStarted() {}

    @Override
    public void rowsParseFinished(long rowsParsed) {}

    @Override
    public void readRowsResponseRequested() {}

    @Override
    public void readRowsResponseObtained(long bytesReceived) {}

    @Override
    public void finished() {}

    @Override
    public void nextBatchNeeded() {}

    @Override
    public BigQueryStorageReadRowsTracer forkWithPrefix(String id) {
      return this;
    }

    @Override
    public long getBytesRead() {
      return 11L;
    }

    @Override
    public long getRowsRead() {
      return 12L;
    }

    @Override
    public long getScanTimeInMilliSec() {
      return 13L;
    }

    @Override
    public long getParseTimeInMilliSec() {
      return 14L;
    }

    @Override
    public long getTimeInSparkInMilliSec() {
      return 15L;
    }
  }
}
