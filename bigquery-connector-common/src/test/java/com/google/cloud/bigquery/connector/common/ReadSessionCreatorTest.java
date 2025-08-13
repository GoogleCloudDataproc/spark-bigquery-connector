/*
 * Copyright 2022 Google LLC
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.testing.LocalChannelProvider;
import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.MockBigQueryRead;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableModifiers;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStub;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ReadSessionCreatorTest {
  EnhancedBigQueryReadStub stub = mock(EnhancedBigQueryReadStub.class);
  BigQueryClient bigQueryClient = mock(BigQueryClient.class);
  UnaryCallable<CreateReadSessionRequest, ReadSession> createReadSessionCall =
      mock(UnaryCallable.class);
  BigQueryReadClient readClient = createMockBigQueryReadClient(stub);
  BigQueryClientFactory bigQueryReadClientFactory = mock(BigQueryClientFactory.class);
  TableInfo table =
      TableInfo.newBuilder(
              TableId.of("a", "b"),
              StandardTableDefinition.newBuilder()
                  .setSchema(Schema.of(Field.of("name", StandardSQLTypeName.BOOL)))
                  .setNumBytes(1L)
                  .build())
          .build();
  TableInfo view =
      TableInfo.newBuilder(
              TableId.of("a", "v"),
              StandardTableDefinition.newBuilder()
                  .setType(TableDefinition.Type.VIEW)
                  .setSchema(Schema.of(Field.of("name", StandardSQLTypeName.BOOL)))
                  .setNumBytes(1L)
                  .build())
          .build();

  private static MockBigQueryRead mockBigQueryRead;
  private static MockServiceHelper mockServiceHelper;
  private LocalChannelProvider channelProvider;
  private BigQueryReadClient client;

  @BeforeClass
  public static void startStaticServer() {
    mockBigQueryRead = new MockBigQueryRead();
    mockServiceHelper =
        new MockServiceHelper(
            UUID.randomUUID().toString(), Arrays.<MockGrpcService>asList(mockBigQueryRead));
    mockServiceHelper.start();
  }

  @AfterClass
  public static void stopServer() {
    mockServiceHelper.stop();
  }

  @Before
  public void setUp() throws IOException {
    mockServiceHelper.reset();
    channelProvider = mockServiceHelper.createChannelProvider();
    BigQueryReadSettings settings =
        BigQueryReadSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();
    client = BigQueryReadClient.create(settings);
  }

  @After
  public void tearDown() throws Exception {
    client.close();
  }

  @Test
  public void testSerializedInstanceIsPropagated() throws Exception {
    TableReadOptions tableReadOptions = TableReadOptions.newBuilder().build();
    ReadSession readSession =
        ReadSession.newBuilder().setName("abc").setReadOptions(tableReadOptions).build();
    CreateReadSessionRequest request =
        CreateReadSessionRequest.newBuilder().setReadSession(readSession).build();
    Optional<String> encodedBase =
        Optional.of(java.util.Base64.getEncoder().encodeToString(request.toByteArray()));
    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder()
            .setEnableReadSessionCaching(false)
            .setRequestEncodedBase(encodedBase)
            .build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, bigQueryReadClientFactory);
    when(bigQueryReadClientFactory.getBigQueryReadClient()).thenReturn(readClient);
    when(bigQueryClient.getTable(any())).thenReturn(table);
    when(stub.createReadSessionCallable()).thenReturn(createReadSessionCall);

    creator
        .create(TableId.of("dataset", "table"), ImmutableList.of("col1", "col2"), Optional.empty())
        .getReadSession();

    ArgumentCaptor<CreateReadSessionRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateReadSessionRequest.class);
    verify(createReadSessionCall, times(1)).call(requestCaptor.capture());

    ReadSession actual = requestCaptor.getValue().getReadSession();
    assertThat(actual.getName()).isEqualTo("abc");
    assertThat(actual.getReadOptions().getSelectedFieldsList()).containsExactly("col1", "col2");
  }

  @Test
  public void testDefaultMinMaxStreamCount() throws Exception {
    // setting up
    when(bigQueryClient.getTable(any())).thenReturn(table);
    mockBigQueryRead.reset();
    mockBigQueryRead.addResponse(
        ReadSession.newBuilder().addStreams(ReadStream.newBuilder().setName("0")).build());
    BigQueryClientFactory mockBigQueryClientFactory = mock(BigQueryClientFactory.class);
    when(mockBigQueryClientFactory.getBigQueryReadClient()).thenReturn(client);

    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder()
            .setEnableReadSessionCaching(false)
            .setDefaultParallelism(10)
            .build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, mockBigQueryClientFactory);
    ReadSessionResponse readSessionResponse =
        creator.create(table.getTableId(), ImmutableList.of(), Optional.empty());
    assertThat(readSessionResponse).isNotNull();
    assertThat(readSessionResponse.getReadSession().getStreamsCount()).isEqualTo(1);
    CreateReadSessionRequest createReadSessionRequest =
        (CreateReadSessionRequest) mockBigQueryRead.getRequests().get(0);
    assertThat(createReadSessionRequest.getMaxStreamCount()).isEqualTo(20_000);
    assertThat(createReadSessionRequest.getPreferredMinStreamCount())
        .isEqualTo(30); // 3 * given default parallelism
  }

  @Test
  public void testCustomMinStreamCount() throws Exception {
    // setting up
    when(bigQueryClient.getTable(any())).thenReturn(table);
    mockBigQueryRead.reset();
    mockBigQueryRead.addResponse(
        ReadSession.newBuilder().addStreams(ReadStream.newBuilder().setName("0")).build());
    BigQueryClientFactory mockBigQueryClientFactory = mock(BigQueryClientFactory.class);
    when(mockBigQueryClientFactory.getBigQueryReadClient()).thenReturn(client);

    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder()
            .setEnableReadSessionCaching(false)
            .setDefaultParallelism(10)
            .setPreferredMinParallelism(OptionalInt.of(21_000))
            .build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, mockBigQueryClientFactory);
    ReadSessionResponse readSessionResponse =
        creator.create(table.getTableId(), ImmutableList.of(), Optional.empty());
    assertThat(readSessionResponse).isNotNull();
    assertThat(readSessionResponse.getReadSession().getStreamsCount()).isEqualTo(1);
    CreateReadSessionRequest createReadSessionRequest =
        (CreateReadSessionRequest) mockBigQueryRead.getRequests().get(0);
    assertThat(createReadSessionRequest.getMaxStreamCount()).isEqualTo(21_000);
    assertThat(createReadSessionRequest.getPreferredMinStreamCount()).isEqualTo(21_000);
  }

  @Test
  public void testCustomMaxStreamCount() throws Exception {
    // setting up
    when(bigQueryClient.getTable(any())).thenReturn(table);
    mockBigQueryRead.reset();
    mockBigQueryRead.addResponse(
        ReadSession.newBuilder().addStreams(ReadStream.newBuilder().setName("0")).build());
    BigQueryClientFactory mockBigQueryClientFactory = mock(BigQueryClientFactory.class);
    when(mockBigQueryClientFactory.getBigQueryReadClient()).thenReturn(client);

    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder()
            .setEnableReadSessionCaching(false)
            .setDefaultParallelism(10)
            .setMaxParallelism(OptionalInt.of(21_000))
            .build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, mockBigQueryClientFactory);
    ReadSessionResponse readSessionResponse =
        creator.create(table.getTableId(), ImmutableList.of(), Optional.empty());
    assertThat(readSessionResponse).isNotNull();
    assertThat(readSessionResponse.getReadSession().getStreamsCount()).isEqualTo(1);
    CreateReadSessionRequest createReadSessionRequest =
        (CreateReadSessionRequest) mockBigQueryRead.getRequests().get(0);
    assertThat(createReadSessionRequest.getMaxStreamCount()).isEqualTo(21_000);
    assertThat(createReadSessionRequest.getPreferredMinStreamCount())
        .isEqualTo(30); // 3 * given default parallelism
  }

  @Test
  public void testMinStreamCountGreaterThanMaxStreamCount() throws Exception {
    // setting up
    when(bigQueryClient.getTable(any())).thenReturn(table);
    mockBigQueryRead.reset();
    mockBigQueryRead.addResponse(
        ReadSession.newBuilder().addStreams(ReadStream.newBuilder().setName("0")).build());
    BigQueryClientFactory mockBigQueryClientFactory = mock(BigQueryClientFactory.class);
    when(mockBigQueryClientFactory.getBigQueryReadClient()).thenReturn(client);

    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder()
            .setEnableReadSessionCaching(false)
            .setPreferredMinParallelism(OptionalInt.of(21_000))
            .setMaxParallelism(OptionalInt.of(10))
            .build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, mockBigQueryClientFactory);
    ReadSessionResponse readSessionResponse =
        creator.create(table.getTableId(), ImmutableList.of(), Optional.empty());
    assertThat(readSessionResponse).isNotNull();
    assertThat(readSessionResponse.getReadSession().getStreamsCount()).isEqualTo(1);
    CreateReadSessionRequest createReadSessionRequest =
        (CreateReadSessionRequest) mockBigQueryRead.getRequests().get(0);
    assertThat(createReadSessionRequest.getMaxStreamCount()).isEqualTo(10);
    assertThat(createReadSessionRequest.getPreferredMinStreamCount()).isEqualTo(10);
  }

  @Test
  public void testMaxStreamCountWithoutMinStreamCount() throws Exception {
    // setting up
    when(bigQueryClient.getTable(any())).thenReturn(table);
    mockBigQueryRead.reset();
    mockBigQueryRead.addResponse(
        ReadSession.newBuilder().addStreams(ReadStream.newBuilder().setName("0")).build());
    BigQueryClientFactory mockBigQueryClientFactory = mock(BigQueryClientFactory.class);
    when(mockBigQueryClientFactory.getBigQueryReadClient()).thenReturn(client);

    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder()
            .setEnableReadSessionCaching(false)
            .setDefaultParallelism(20)
            .setMaxParallelism(OptionalInt.of(10))
            .build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, mockBigQueryClientFactory);
    ReadSessionResponse readSessionResponse =
        creator.create(table.getTableId(), ImmutableList.of(), Optional.empty());
    assertThat(readSessionResponse).isNotNull();
    assertThat(readSessionResponse.getReadSession().getStreamsCount()).isEqualTo(1);
    CreateReadSessionRequest createReadSessionRequest =
        (CreateReadSessionRequest) mockBigQueryRead.getRequests().get(0);
    assertThat(createReadSessionRequest.getMaxStreamCount()).isEqualTo(10);
    assertThat(createReadSessionRequest.getPreferredMinStreamCount()).isEqualTo(10);
  }

  @Test
  public void testSnapshotTimeMillis() throws Exception {
    // setting up
    when(bigQueryClient.getTable(any())).thenReturn(table);
    mockBigQueryRead.reset();
    mockBigQueryRead.addResponse(
        ReadSession.newBuilder().addStreams(ReadStream.newBuilder().setName("0")).build());
    BigQueryClientFactory mockBigQueryClientFactory = mock(BigQueryClientFactory.class);
    when(mockBigQueryClientFactory.getBigQueryReadClient()).thenReturn(client);

    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder()
            .setEnableReadSessionCaching(false)
            .setSnapshotTimeMillis(OptionalLong.of(1234567890L))
            .build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, mockBigQueryClientFactory);
    ReadSessionResponse readSessionResponse =
        creator.create(table.getTableId(), ImmutableList.of(), Optional.empty());
    assertThat(readSessionResponse).isNotNull();
    CreateReadSessionRequest createReadSessionRequest =
        (CreateReadSessionRequest) mockBigQueryRead.getRequests().get(0);
    assertThat(createReadSessionRequest.getReadSession().getTableModifiers().getSnapshotTime())
        .isEqualTo(Timestamp.newBuilder().setSeconds(1234567).setNanos(890000000).build());
  }

  @Test
  public void testViewSnapshotTimeMillis() throws Exception {
    // setting up
    String query = "SELECT * FROM `a.v`";
    when(bigQueryClient.getTable(any())).thenReturn(view);
    when(bigQueryClient.createSql(
            view.getTableId(), ImmutableList.of(), new String[0], OptionalLong.of(1234567890L)))
        .thenReturn(query);
    when(bigQueryClient.materializeViewToTable(query, view.getTableId())).thenReturn(table);
    mockBigQueryRead.reset();
    mockBigQueryRead.addResponse(
        ReadSession.newBuilder().addStreams(ReadStream.newBuilder().setName("0")).build());
    BigQueryClientFactory mockBigQueryClientFactory = mock(BigQueryClientFactory.class);
    when(mockBigQueryClientFactory.getBigQueryReadClient()).thenReturn(client);

    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder()
            .setEnableReadSessionCaching(false)
            .setSnapshotTimeMillis(OptionalLong.of(1234567890L))
            .setViewsEnabled(true)
            .build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, mockBigQueryClientFactory);
    ReadSessionResponse readSessionResponse =
        creator.create(table.getTableId(), ImmutableList.of(), Optional.empty());
    assertThat(readSessionResponse).isNotNull();
    CreateReadSessionRequest createReadSessionRequest =
        (CreateReadSessionRequest) mockBigQueryRead.getRequests().get(0);
    assertThat(createReadSessionRequest.getReadSession().getTableModifiers())
        .isEqualTo(TableModifiers.newBuilder().build());
  }

  private void testCacheMissScenario(
      ReadSessionCreator creator,
      String readSessionName,
      ImmutableList<String> fields,
      Optional<String> filter) {
    ReadSession readSession = ReadSession.newBuilder().setName(readSessionName).build();
    mockBigQueryRead.addResponse(readSession);
    ReadSessionResponse response = creator.create(table.getTableId(), fields, filter);
    assertThat(creator.getReadSessionCache().asMap().values()).contains(readSession);
    assertThat(response.getReadSession()).isEqualTo(readSession);
  }

  @Test
  public void testReadSessionCacheMiss() {
    // setting up
    when(bigQueryClient.getTable(any())).thenReturn(table);
    mockBigQueryRead.reset();
    BigQueryClientFactory mockBigQueryClientFactory = mock(BigQueryClientFactory.class);
    when(mockBigQueryClientFactory.getBigQueryReadClient()).thenReturn(client);
    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder()
            .setMaxParallelism(OptionalInt.of(10))
            .setReadDataFormat(DataFormat.ARROW)
            .setEnableReadSessionCaching(true)
            .build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, mockBigQueryClientFactory);
    creator = Mockito.spy(creator);
    Cache<CreateReadSessionRequest, ReadSession> cache = CacheBuilder.newBuilder().build();
    Mockito.doReturn(cache).when(creator).getReadSessionCache();
    int counter = 0;
    testCacheMissScenario(creator, "rs" + ++counter, ImmutableList.of(), Optional.empty());
    testCacheMissScenario(
        creator, "rs" + ++counter, ImmutableList.of("foo", "bar"), Optional.empty());
    testCacheMissScenario(
        creator, "rs" + ++counter, ImmutableList.of("foo", "baz1"), Optional.empty());
    testCacheMissScenario(creator, "rs" + ++counter, ImmutableList.of(), Optional.of("filter1"));
    testCacheMissScenario(creator, "rs" + ++counter, ImmutableList.of(), Optional.of("filter2"));
    testCacheMissScenario(
        creator, "rs" + ++counter, ImmutableList.of("foo", "bar"), Optional.of("filter1"));
    testCacheMissScenario(
        creator, "rs" + ++counter, ImmutableList.of("foo", "bar"), Optional.of("filter2"));
  }

  private ReadSession addCacheEntry(
      ReadSessionCreator creator,
      String readSessionName,
      ImmutableList<String> fields,
      Optional<String> filter,
      ReadSessionCreatorConfig config) {
    ReadSession readSession = ReadSession.newBuilder().setName(readSessionName).build();
    CreateReadSessionRequest request = CreateReadSessionRequest.newBuilder().build();
    ReadSession.Builder requestedSession = request.getReadSession().toBuilder();
    TableReadOptions.Builder readOptions = requestedSession.getReadOptionsBuilder();
    filter.ifPresent(readOptions::setRowRestriction);
    readOptions.addAllSelectedFields(fields);
    readOptions.setArrowSerializationOptions(
        ArrowSerializationOptions.newBuilder()
            .setBufferCompression(config.getArrowCompressionCodec())
            .build());
    readOptions.setResponseCompressionCodec(config.getResponseCompressionCodec());
    CreateReadSessionRequest key =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/" + bigQueryClient.getProjectId())
            .setReadSession(
                requestedSession
                    .setDataFormat(config.getReadDataFormat())
                    .setReadOptions(readOptions)
                    .setTable(ReadSessionCreator.toTablePath(table.getTableId()))
                    .setTableModifiers(TableModifiers.newBuilder())
                    .build())
            .setMaxStreamCount(config.getMaxParallelism().getAsInt())
            .setPreferredMinStreamCount(3 * config.getDefaultParallelism())
            .build();
    creator.getReadSessionCache().put(key, readSession);
    return readSession;
  }

  private void testCacheHitScenario(
      ReadSessionCreator creator,
      ReadSession expected,
      ImmutableList<String> fields,
      Optional<String> filter) {
    ReadSessionResponse response = creator.create(table.getTableId(), fields, filter);
    assertThat(response.getReadSession()).isEqualTo(expected);
  }

  @Test
  public void testReadSessionCacheHit() {
    // setting up
    when(bigQueryClient.getTable(any())).thenReturn(table);
    mockBigQueryRead.reset();
    mockBigQueryRead.addResponse(ReadSession.newBuilder().setName("wrong-name").build());
    // mockBigQueryRead.addException(new RuntimeException("Test: Cached ReadSession not used"));
    BigQueryClientFactory mockBigQueryClientFactory = mock(BigQueryClientFactory.class);
    when(mockBigQueryClientFactory.getBigQueryReadClient()).thenReturn(client);
    int maxStreamCount = 20_000;
    int defaultParallelism = 10;
    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder()
            .setDefaultParallelism(defaultParallelism)
            .setMaxParallelism(OptionalInt.of(maxStreamCount))
            .setReadDataFormat(DataFormat.ARROW)
            .setEnableReadSessionCaching(true)
            .setArrowCompressionCodec(CompressionCodec.COMPRESSION_UNSPECIFIED)
            .setResponseCompressionCodec(
                ResponseCompressionCodec.RESPONSE_COMPRESSION_CODEC_UNSPECIFIED)
            .build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, mockBigQueryClientFactory);
    creator = Mockito.spy(creator);
    Cache<CreateReadSessionRequest, ReadSession> cache = CacheBuilder.newBuilder().build();
    Mockito.doReturn(cache).when(creator).getReadSessionCache();
    // test cache hits
    ReadSession expected1 =
        addCacheEntry(creator, "r1", ImmutableList.of(), Optional.empty(), config);
    testCacheHitScenario(creator, expected1, ImmutableList.of(), Optional.empty());
    ReadSession expected2 =
        addCacheEntry(
            creator, "r2", ImmutableList.of("foo", "bar"), Optional.of("filter1"), config);
    testCacheHitScenario(
        creator, expected2, ImmutableList.of("foo", "bar"), Optional.of("filter1"));
    ReadSession expected3 =
        addCacheEntry(
            creator, "r3", ImmutableList.of("foo", "bar"), Optional.of("filter2"), config);
    testCacheHitScenario(
        creator, expected3, ImmutableList.of("foo", "bar"), Optional.of("filter2"));
    // re-test previous cache hits
    testCacheHitScenario(creator, expected1, ImmutableList.of(), Optional.empty());
    testCacheHitScenario(
        creator, expected2, ImmutableList.of("foo", "bar"), Optional.of("filter1"));
    testCacheHitScenario(
        creator, expected3, ImmutableList.of("foo", "bar"), Optional.of("filter2"));
  }

  private static BigQueryReadClient createMockBigQueryReadClient(EnhancedBigQueryReadStub stub) {
    try {
      BigQueryReadClient client = BigQueryReadClient.create(stub);
      // settings is null, causing NPE in some of the tests
      FieldUtils.writeField(client, "settings", BigQueryReadSettings.newBuilder().build(), true);
      return client;
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
