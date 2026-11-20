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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.cloud.spark.bigquery.v2.context.ArrowInputPartitionContext;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.cloud.spark.bigquery.v2.context.InputPartitionReaderContext;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.Test;

public class Spark32BigQueryScanBuilderTest {

  @Test
  public void supportedCustomMetricsIncludesEstimatedBytesScanned() {
    Spark32BigQueryScanBuilder scanBuilder = new Spark32BigQueryScanBuilder(null);

    assertThat(
            Arrays.stream(scanBuilder.supportedCustomMetrics())
                .map(CustomMetric::name)
                .collect(Collectors.toList()))
        .containsExactly(
            "bqBytesRead",
            "bqRowsRead",
            "bqScanTime",
            "bqParseTime",
            "bqTimeInSpark",
            "bqNumReadStreams",
            "bqEstimatedBytesScanned")
        .inOrder();
  }

  @Test
  public void runtimeFilteringUsesReplacementSessionEstimate() throws IOException {
    BigQueryDataSourceReaderContext context = mock(BigQueryDataSourceReaderContext.class);
    ArrowInputPartitionContext initialPartition = arrowPartition(4096L);
    ArrowInputPartitionContext replacementPartition = arrowPartition(2048L);
    Filter[] filters = {new EqualTo("partitionColumn", 1)};
    when(context.enableBatchRead()).thenReturn(true);
    when(context.planBatchInputPartitionContexts()).thenReturn(Stream.of(initialPartition));
    when(context.filter(filters)).thenReturn(Optional.of(ImmutableList.of(replacementPartition)));
    Spark32BigQueryScanBuilder scanBuilder = new Spark32BigQueryScanBuilder(context);
    PartitionReaderFactory readerFactory = scanBuilder.createReaderFactory();

    assertEstimatedBytesScanned(readerFactory, scanBuilder.planInputPartitions(), 4096L);

    scanBuilder.filter(filters);

    assertEstimatedBytesScanned(readerFactory, scanBuilder.planInputPartitions(), 2048L);
  }

  private static void assertEstimatedBytesScanned(
      PartitionReaderFactory readerFactory, InputPartition[] partitions, long expectedBytes)
      throws IOException {
    assertThat(partitions).hasLength(1);
    try (PartitionReader<ColumnarBatch> reader =
        readerFactory.createColumnarReader(partitions[0])) {
      assertThat(
              Arrays.stream(reader.currentMetricsValues())
                  .filter(metric -> metric.name().equals("bqEstimatedBytesScanned"))
                  .map(CustomTaskMetric::value)
                  .collect(Collectors.toList()))
          .containsExactly(expectedBytes);
    }
  }

  private static ArrowInputPartitionContext arrowPartition(long estimatedBytesScanned) {
    ReadSession readSession =
        ReadSession.newBuilder().setEstimatedTotalBytesScanned(estimatedBytesScanned).build();
    ArrowInputPartitionContext partition =
        spy(
            new ArrowInputPartitionContext(
                /* bigQueryClientFactory= */ null,
                /* tracerFactory= */ null,
                ImmutableList.of("streamName"),
                new ReadRowsHelper.Options(
                    /* maxRetries= */ 5,
                    Optional.of("endpoint"),
                    /* backgroundParsingThreads= */ 5,
                    /* prebufferResponses= */ 1),
                ImmutableList.of(),
                new ReadSessionResponse(readSession, null),
                Optional.empty(),
                /* sparkBigQueryReadSessionMetrics= */ null,
                ResponseCompressionCodec.RESPONSE_COMPRESSION_CODEC_UNSPECIFIED));
    // Stub the executor-side context so the reader can report metrics without opening a stream.
    doReturn(mock(InputPartitionReaderContext.class))
        .when(partition)
        .createPartitionReaderContext();
    return partition;
  }
}
