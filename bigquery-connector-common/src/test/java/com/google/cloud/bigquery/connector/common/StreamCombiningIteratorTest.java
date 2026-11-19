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
package com.google.cloud.bigquery.connector.common;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadGrpc.BigQueryReadImplBase;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Reads from a fake Read API served in-process, with streams that fail with a retryable error, to
 * check that the iterator keeps requesting responses from the retried streams.
 */
public class StreamCombiningIteratorTest {

  private static final String STREAM = "projects/p/locations/us/sessions/s/streams/0";
  private static final long ROWS_PER_RESPONSE = 10;
  private static final long TIMEOUT_SECONDS = 10;

  private final FakeReadService service = new FakeReadService();
  private Server server;
  private BigQueryReadClient client;
  private StreamCombiningIterator iterator;

  @Before
  public void setUp() throws Exception {
    String serverName = InProcessServerBuilder.generateName();
    // Default executors, so that callbacks run on other threads as with the Netty transport.
    server = InProcessServerBuilder.forName(serverName).addService(service).build().start();

    InstantiatingGrpcChannelProvider channelProvider =
        BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
            .setChannelConfigurator(ignored -> InProcessChannelBuilder.forName(serverName))
            .build();
    // Set GAX's own ReadRows maxAttempts to 1 so retryable INTERNAL errors reach
    // StreamCombiningIterator.Observer.onError rather than being retried inside GAX.
    BigQueryReadSettings.Builder settingsBuilder =
        BigQueryReadSettings.newBuilder()
            .setCredentialsProvider(NoCredentialsProvider.create())
            .setTransportChannelProvider(channelProvider);
    settingsBuilder
        .readRowsSettings()
        .setRetrySettings(
            settingsBuilder
                .readRowsSettings()
                .getRetrySettings()
                .toBuilder()
                .setMaxAttempts(1)
                .build());
    client = BigQueryReadClient.create(settingsBuilder.build());
  }

  @After
  public void tearDown() throws Exception {
    if (iterator != null) {
      iterator.cancel();
    }
    client.close();
    client.awaitTermination(5, TimeUnit.SECONDS);
    server.shutdownNow();
    server.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test(timeout = 30_000)
  public void readsTheRetriedStreamWhenTheFirstAttemptFailsBeforeAnyResponse() throws Exception {
    service.attempts.add(responseObserver -> responseObserver.onError(retryableError()));
    service.attempts.add(responseObserver -> sendAndComplete(responseObserver, 10));
    iterator = newIterator(/* bufferEntriesPerStream= */ 4);

    assertThat(readAll()).isEqualTo(10);
    assertThat(service.offsets).containsExactly(0L, 0L).inOrder();
  }

  @Test(timeout = 30_000)
  public void readsTheRetriedStreamWhenTheFirstAttemptFailsAfterConsumedResponses()
      throws Exception {
    CountDownLatch consumed = new CountDownLatch(1);
    service.attempts.add(
        responseObserver -> {
          send(responseObserver, 3);
          await(consumed);
          responseObserver.onError(retryableError());
        });
    service.attempts.add(responseObserver -> sendAndComplete(responseObserver, 10));
    iterator = newIterator(/* bufferEntriesPerStream= */ 8);

    for (int i = 0; i < 3; i++) {
      assertThat(iterator.hasNext()).isTrue();
      iterator.next();
    }
    consumed.countDown();

    assertThat(readAll()).isEqualTo(10);
    assertThat(service.offsets).containsExactly(0L, 3 * ROWS_PER_RESPONSE).inOrder();
  }

  private StreamCombiningIterator newIterator(int bufferEntriesPerStream) {
    return new StreamCombiningIterator(
        client,
        ImmutableList.of(ReadRowsRequest.newBuilder().setReadStream(STREAM)),
        bufferEntriesPerStream,
        /* numRetries= */ 1);
  }

  /** Reads the remaining responses, checks their row counts, and returns how many there were. */
  private int readAll() {
    int responses = 0;
    while (iterator.hasNext()) {
      assertThat(iterator.next().getRowCount()).isEqualTo(ROWS_PER_RESPONSE);
      responses++;
    }
    return responses;
  }

  private static void send(StreamObserver<ReadRowsResponse> responseObserver, int responses) {
    for (int i = 0; i < responses; i++) {
      responseObserver.onNext(ReadRowsResponse.newBuilder().setRowCount(ROWS_PER_RESPONSE).build());
    }
  }

  private static void sendAndComplete(
      StreamObserver<ReadRowsResponse> responseObserver, int responses) {
    send(responseObserver, responses);
    responseObserver.onCompleted();
  }

  private static RuntimeException retryableError() {
    return Status.INTERNAL
        .withDescription("Received unexpected EOS on DATA frame from server")
        .asRuntimeException();
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Timed out waiting for the test to proceed");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private interface Attempt {
    void run(StreamObserver<ReadRowsResponse> responseObserver);
  }

  /** Fake Read API that runs the next scripted attempt for each ReadRows call. */
  private static class FakeReadService extends BigQueryReadImplBase {
    final List<Attempt> attempts = new CopyOnWriteArrayList<>();
    final List<Long> offsets = new CopyOnWriteArrayList<>();

    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
      int attempt = offsets.size();
      offsets.add(request.getOffset());
      attempts.get(attempt).run(responseObserver);
    }
  }
}
