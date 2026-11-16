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
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadGrpc.BigQueryReadImplBase;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Checks which channel of the client's channel pool each ReadRows call is sent on, using the gax
 * channel pool over in-process channels that tag their calls with the channel's index.
 */
public class StreamCombiningIteratorChannelSelectionTest {

  private static final int POOL_SIZE = 3;
  private static final String STREAM_PREFIX = "projects/p/locations/us/sessions/s/streams/";
  private static final Metadata.Key<String> CHANNEL_HEADER =
      Metadata.Key.of("x-test-channel", Metadata.ASCII_STRING_MARSHALLER);
  private static final Context.Key<String> CHANNEL_CONTEXT_KEY = Context.key("test-channel");
  private static final long TIMEOUT_SECONDS = 10;

  private FakeReadService service;
  private Server server;
  private BigQueryReadClient client;
  private final AtomicInteger callsUntilSyncFailure = new AtomicInteger(-1);
  private final List<StreamCombiningIterator> iterators = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    service = new FakeReadService();
    String serverName = InProcessServerBuilder.generateName();
    ServerInterceptor channelHeaderToContext =
        new ServerInterceptor() {
          @Override
          public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
              ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
            Context context =
                Context.current().withValue(CHANNEL_CONTEXT_KEY, headers.get(CHANNEL_HEADER));
            return Contexts.interceptCall(context, call, headers, next);
          }
        };
    // Default executors, so that callbacks run on other threads as with the Netty transport.
    server =
        InProcessServerBuilder.forName(serverName)
            .addService(ServerInterceptors.intercept(service, channelHeaderToContext))
            .build()
            .start();

    ClientInterceptor syncFailureInterceptor =
        new ClientInterceptor() {
          @Override
          public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
              MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            int remaining = callsUntilSyncFailure.getAndUpdate(n -> n >= 0 ? n - 1 : n);
            if (remaining == 0) {
              throw new IllegalStateException("Simulated synchronous call failure");
            }
            return next.newCall(method, callOptions);
          }
        };

    // The pool creates its channels in order, so channel i is the pool entry selected by
    // affinity i.
    AtomicInteger nextChannelIndex = new AtomicInteger();
    InstantiatingGrpcChannelProvider channelProvider =
        BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
            .setChannelPoolSettings(ChannelPoolSettings.staticallySized(POOL_SIZE))
            .setChannelConfigurator(
                ignored -> {
                  Metadata headers = new Metadata();
                  headers.put(CHANNEL_HEADER, String.valueOf(nextChannelIndex.getAndIncrement()));
                  return InProcessChannelBuilder.forName(serverName)
                      .intercept(
                          MetadataUtils.newAttachHeadersInterceptor(headers),
                          syncFailureInterceptor);
                })
            .build();
    // Set GAX's own ReadRows maxAttempts to 1 so retryable INTERNAL errors reach
    // StreamCombiningIterator.Observer.onError rather than being retried inside GAX on the same
    // GrpcCallContext.
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
    assertThat(nextChannelIndex.get()).isEqualTo(POOL_SIZE);
  }

  @After
  public void tearDown() throws Exception {
    iterators.forEach(StreamCombiningIterator::cancel);
    service.completeOpenStreams();
    client.close();
    client.awaitTermination(5, TimeUnit.SECONDS);
    server.shutdownNow();
    server.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test(timeout = 60_000)
  public void roundRobinPlacesShortStreamsOnTheChannelOfALongRunningStream() throws Exception {
    int longRunningChannel = startLongRunningStream("long", Optional.empty());

    List<Integer> shortStreamChannels = readShortStreams(POOL_SIZE, Optional.empty());

    // Documents the behavior without a selector: round-robin wraps around to the busy channel.
    assertThat(shortStreamChannels).contains(longRunningChannel);
  }

  @Test(timeout = 60_000)
  public void selectorKeepsShortStreamsOffTheChannelOfALongRunningStream() throws Exception {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(POOL_SIZE);
    int longRunningChannel = startLongRunningStream("long", Optional.of(selector));

    List<Integer> shortStreamChannels = readShortStreams(10, Optional.of(selector));

    assertThat(shortStreamChannels).doesNotContain(longRunningChannel);
    assertThat(selector.getActiveStreams()[longRunningChannel]).isEqualTo(1);
  }

  @Test(timeout = 60_000)
  public void roundRobinCanPlaceLongRunningStreamsOnTheSameChannel() throws Exception {
    List<Integer> longRunningChannels =
        startLongRunningStreamsBetweenShortStreams(Optional.empty());

    assertThat(longRunningChannels.stream().distinct().count()).isLessThan(POOL_SIZE);
  }

  @Test(timeout = 60_000)
  public void selectorPlacesLongRunningStreamsOnSeparateChannels() throws Exception {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(POOL_SIZE);

    List<Integer> longRunningChannels =
        startLongRunningStreamsBetweenShortStreams(Optional.of(selector));

    assertThat(longRunningChannels).containsExactly(0, 1, 2);
    assertThat(selector.getActiveStreams()).asList().containsExactly(1, 1, 1);
  }

  @Test(timeout = 60_000)
  public void callIsSentOnTheChannelReturnedByTheSelector() throws Exception {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(POOL_SIZE);
    // Occupies channels 0 and 1, so the selector returns channel 2 next.
    startLongRunningStream("long-0", Optional.of(selector));
    startLongRunningStream("long-1", Optional.of(selector));

    assertThat(startLongRunningStream("long-2", Optional.of(selector))).isEqualTo(2);
  }

  @Test(timeout = 60_000)
  public void releasesTheChannelWhenTheStreamCompletes() throws Exception {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(POOL_SIZE);

    readShortStreams(5, Optional.of(selector));

    assertThat(selector.getActiveStreams()).asList().containsExactly(0, 0, 0);
  }

  @Test(timeout = 60_000)
  public void releasesTheChannelWhenTheStreamIsCancelled() throws Exception {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(POOL_SIZE);
    startLongRunningStream("long", Optional.of(selector));
    assertThat(selector.getActiveStreams()).asList().containsExactly(1, 0, 0);

    iterators.forEach(StreamCombiningIterator::cancel);

    assertThat(selector.getActiveStreams()).asList().containsExactly(0, 0, 0);
  }

  @Test(timeout = 60_000)
  public void releasesTheChannelWhenTheStreamFails() throws Exception {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(POOL_SIZE);
    StreamCombiningIterator iterator = newIterator("failing", Optional.of(selector));

    assertThrows(RuntimeException.class, iterator::hasNext);
    assertThat(selector.getActiveStreams()).asList().containsExactly(0, 0, 0);
  }

  @Test(timeout = 60_000)
  public void releasesThePreviousChannelAndPicksAnotherWhenRetried() throws Exception {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(POOL_SIZE);
    StreamCombiningIterator iterator =
        newIterator(ImmutableList.of("flaky"), /* numRetries= */ 1, Optional.of(selector));

    assertThat(iterator.hasNext()).isTrue();
    service.awaitAttempts("flaky", 2);
    iterator.next();
    assertThat(iterator.hasNext()).isTrue();
    iterator.next();
    assertThat(iterator.hasNext()).isFalse();

    assertThat(service.channelsOf("flaky")).containsExactly(0, 1).inOrder();
    assertThat(selector.getActiveStreams()).asList().containsExactly(0, 0, 0);
  }

  @Test(timeout = 60_000)
  public void releasesChannelsWhenStartingASecondStreamFailsSynchronously() throws Exception {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(POOL_SIZE);
    callsUntilSyncFailure.set(1);

    assertThrows(
        IllegalStateException.class,
        () -> newIterator(ImmutableList.of("long-0", "long-1"), Optional.of(selector)));
    assertThat(selector.getActiveStreams()).asList().containsExactly(0, 0, 0);
  }

  @Test(timeout = 60_000)
  public void releasesTheChannelsOfAllStreamsCombinedInOneIterator() throws Exception {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(POOL_SIZE);
    StreamCombiningIterator iterator =
        newIterator(ImmutableList.of("short-a", "short-b", "short-c"), Optional.of(selector));

    int responses = 0;
    while (iterator.hasNext()) {
      iterator.next();
      responses++;
    }

    assertThat(responses).isEqualTo(3);
    assertThat(service.channelOf("short-a")).isNotEqualTo(service.channelOf("short-b"));
    assertThat(selector.getActiveStreams()).asList().containsExactly(0, 0, 0);
  }

  @Test(timeout = 60_000)
  public void readRowsHelperUsesTheSelectorOfTheClientFactory() throws Exception {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(POOL_SIZE);
    BigQueryClientFactory clientFactory = mock(BigQueryClientFactory.class);
    when(clientFactory.getBigQueryReadClient()).thenReturn(client);
    when(clientFactory.getReadRowsChannelSelector()).thenReturn(selector);
    // Occupies channel 0, so the helper's stream must go to channel 1.
    startLongRunningStream("long-0", Optional.of(selector));

    ReadRowsHelper helper =
        new ReadRowsHelper(
            clientFactory,
            ReadRowsRequest.newBuilder().setReadStream(STREAM_PREFIX + "long-helper"),
            new ReadRowsHelper.Options(
                /* maxReadRowsRetries= */ 0,
                Optional.empty(),
                /* backgroundParsingThreads= */ 0,
                /* prebufferResponses= */ 1),
            Optional.empty());
    helper.readRows();

    assertThat(service.awaitChannelOf("long-helper")).isEqualTo(1);
    assertThat(selector.getActiveStreams()).asList().containsExactly(1, 1, 0).inOrder();
    helper.close();
    assertThat(selector.getActiveStreams()).asList().containsExactly(1, 0, 0).inOrder();
  }

  @Test(timeout = 60_000)
  public void readRowsHelperWorksWithoutASelector() throws Exception {
    BigQueryClientFactory clientFactory = mock(BigQueryClientFactory.class);
    when(clientFactory.getBigQueryReadClient()).thenReturn(client);

    ReadRowsHelper helper =
        new ReadRowsHelper(
            clientFactory,
            ReadRowsRequest.newBuilder().setReadStream(STREAM_PREFIX + "short-helper"),
            new ReadRowsHelper.Options(
                /* maxReadRowsRetries= */ 0,
                Optional.empty(),
                /* backgroundParsingThreads= */ 0,
                /* prebufferResponses= */ 1),
            Optional.empty());
    try {
      assertThat(ImmutableList.copyOf(helper.readRows())).hasSize(1);
    } finally {
      helper.close();
    }
  }

  /**
   * Starts a long-running stream, then two short streams that complete, then two more long-running
   * streams, and returns the channels of the three long-running streams.
   */
  private List<Integer> startLongRunningStreamsBetweenShortStreams(
      Optional<ReadRowsChannelSelector> selector) throws Exception {
    List<Integer> channels = new ArrayList<>();
    channels.add(startLongRunningStream("long-0", selector));
    readShortStreams(2, selector);
    channels.add(startLongRunningStream("long-1", selector));
    channels.add(startLongRunningStream("long-2", selector));
    return channels;
  }

  /** Starts a stream that the server keeps open, and returns the channel it was sent on. */
  private int startLongRunningStream(String name, Optional<ReadRowsChannelSelector> selector)
      throws Exception {
    newIterator(name, selector);
    return service.awaitChannelOf(name);
  }

  /** Reads streams that each return one response, one after the other, and returns channels. */
  private List<Integer> readShortStreams(int count, Optional<ReadRowsChannelSelector> selector)
      throws Exception {
    List<Integer> channels = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String name = "short-" + service.nextShortStreamId.getAndIncrement();
      StreamCombiningIterator iterator = newIterator(name, selector);
      assertThat(iterator.hasNext()).isTrue();
      iterator.next();
      assertThat(iterator.hasNext()).isFalse();
      channels.add(service.channelOf(name));
    }
    return channels;
  }

  private StreamCombiningIterator newIterator(
      String name, Optional<ReadRowsChannelSelector> selector) {
    return newIterator(ImmutableList.of(name), selector);
  }

  private StreamCombiningIterator newIterator(
      List<String> names, Optional<ReadRowsChannelSelector> selector) {
    return newIterator(names, /* numRetries= */ 0, selector);
  }

  private StreamCombiningIterator newIterator(
      List<String> names, int numRetries, Optional<ReadRowsChannelSelector> selector) {
    List<ReadRowsRequest.Builder> requests = new ArrayList<>();
    for (String name : names) {
      requests.add(ReadRowsRequest.newBuilder().setReadStream(STREAM_PREFIX + name));
    }
    StreamCombiningIterator iterator =
        new StreamCombiningIterator(
            client, requests, /* bufferEntriesPerStream= */ 1, numRetries, selector);
    iterators.add(iterator);
    return iterator;
  }

  /**
   * Fake Read API that records the channel each stream was read on. Streams named "long*" stay open
   * until the end of the test, streams named "failing" fail, streams named "flaky" fail once with a
   * retryable error before succeeding, and other streams return one response.
   */
  private static class FakeReadService extends BigQueryReadImplBase {
    final AtomicInteger nextShortStreamId = new AtomicInteger();
    private final Map<String, List<Integer>> channelsByStream = new ConcurrentHashMap<>();
    private final List<StreamObserver<ReadRowsResponse>> openStreams = new CopyOnWriteArrayList<>();

    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
      String name = request.getReadStream().substring(STREAM_PREFIX.length());
      int channel = Integer.parseInt(CHANNEL_CONTEXT_KEY.get());
      List<Integer> channels =
          channelsByStream.compute(
              name,
              (ignored, existing) -> {
                List<Integer> updated = existing != null ? existing : new CopyOnWriteArrayList<>();
                updated.add(channel);
                return updated;
              });
      if (name.startsWith("long")) {
        openStreams.add(responseObserver);
      } else if (name.equals("failing")) {
        responseObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException());
      } else if (name.equals("flaky") && channels.size() == 1) {
        responseObserver.onNext(ReadRowsResponse.newBuilder().setRowCount(1).build());
        responseObserver.onError(
            Status.INTERNAL
                .withDescription("Connection closed with unknown cause")
                .asRuntimeException());
      } else {
        responseObserver.onNext(ReadRowsResponse.newBuilder().setRowCount(1).build());
        responseObserver.onCompleted();
      }
    }

    int channelOf(String name) {
      List<Integer> channels = channelsOf(name);
      assertThat(channels).hasSize(1);
      return channels.get(0);
    }

    List<Integer> channelsOf(String name) {
      List<Integer> channels = channelsByStream.get(name);
      assertThat(channels).isNotNull();
      return channels;
    }

    int awaitChannelOf(String name) throws InterruptedException {
      awaitAttempts(name, 1);
      return channelOf(name);
    }

    void awaitAttempts(String name, int expectedAttempts) throws InterruptedException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
      while (System.nanoTime() < deadline) {
        List<Integer> channels = channelsByStream.get(name);
        if (channels != null && channels.size() >= expectedAttempts) {
          return;
        }
        Thread.sleep(10);
      }
      assertThat(channelsOf(name)).hasSize(expectedAttempts);
    }

    void completeOpenStreams() {
      for (StreamObserver<ReadRowsResponse> stream : openStreams) {
        try {
          stream.onCompleted();
        } catch (RuntimeException e) {
          // The call may already be cancelled by the client.
        }
      }
    }
  }
}
