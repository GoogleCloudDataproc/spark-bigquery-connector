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

package com.google.cloud.spark.bigquery.v2.context;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Optional;
import org.junit.Test;

public class InputPartitionContextTest {

  private static final long ESTIMATED_BYTES_SCANNED = 4096L;

  @Test
  public void arrowContextRetainsEstimateWhenSerialized() throws Exception {
    ArrowInputPartitionContext context =
        new ArrowInputPartitionContext(
            /* bigQueryClientFactory= */ null,
            /* tracerFactory= */ null,
            ImmutableList.of("streamName"),
            readRowsOptions(),
            ImmutableList.of(),
            readSessionResponse(),
            Optional.empty(),
            /* sparkBigQueryReadSessionMetrics= */ null,
            ResponseCompressionCodec.RESPONSE_COMPRESSION_CODEC_UNSPECIFIED);

    ArrowInputPartitionContext deserialized = serializeAndDeserialize(context);

    assertThat(deserialized.getEstimatedBytesScanned()).hasValue(ESTIMATED_BYTES_SCANNED);
  }

  @Test
  public void avroContextRetainsEstimateWhenSerialized() throws Exception {
    BigQueryInputPartitionContext context =
        new BigQueryInputPartitionContext(
            /* bigQueryReadClientFactory= */ null,
            "streamName",
            readRowsOptions(),
            /* converter= */ null,
            ESTIMATED_BYTES_SCANNED);

    BigQueryInputPartitionContext deserialized = serializeAndDeserialize(context);

    assertThat(deserialized.getEstimatedBytesScanned()).hasValue(ESTIMATED_BYTES_SCANNED);
  }

  @Test
  public void emptyProjectionContextHasNoEstimate() {
    assertThat(new EmptyProjectionInputPartitionContext(1).getEstimatedBytesScanned()).isEmpty();
  }

  private static ReadRowsHelper.Options readRowsOptions() {
    return new ReadRowsHelper.Options(
        /* maxRetries= */ 5,
        Optional.of("endpoint"),
        /* backgroundParsingThreads= */ 5,
        /* prebufferResponses= */ 1);
  }

  private static ReadSessionResponse readSessionResponse() {
    ReadSession readSession =
        ReadSession.newBuilder().setEstimatedTotalBytesScanned(ESTIMATED_BYTES_SCANNED).build();
    return new ReadSessionResponse(readSession, null);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Serializable> T serializeAndDeserialize(T value)
      throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(value);
    }
    try (ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) input.readObject();
    }
  }
}
