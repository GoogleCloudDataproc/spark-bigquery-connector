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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.cloud.spark.bigquery.v2.context.ArrowInputPartitionContext;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.sources.Filter;
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
  public void runtimeFilteringUsesReplacementSessionEstimate() {
    BigQueryDataSourceReaderContext context = mock(BigQueryDataSourceReaderContext.class);
    ArrowInputPartitionContext replacementPartition = arrowPartition(2048L);
    when(context.filter(any(Filter[].class)))
        .thenReturn(Optional.of(ImmutableList.of(replacementPartition)));
    Spark32BigQueryScanBuilder scanBuilder = new Spark32BigQueryScanBuilder(context);

    scanBuilder.filter(new Filter[] {});
    InputPartition[] partitions = scanBuilder.planInputPartitions();

    assertThat(((BigQueryInputPartition) partitions[0]).getContext().getEstimatedBytesScanned())
        .hasValue(2048L);
  }

  private static ArrowInputPartitionContext arrowPartition(long estimatedBytesScanned) {
    ReadSession readSession =
        ReadSession.newBuilder().setEstimatedTotalBytesScanned(estimatedBytesScanned).build();
    return new ArrowInputPartitionContext(
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
        ResponseCompressionCodec.RESPONSE_COMPRESSION_CODEC_UNSPECIFIED);
  }
}
