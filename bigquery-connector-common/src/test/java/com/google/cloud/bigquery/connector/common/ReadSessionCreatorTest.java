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
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.MockBigQueryRead;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStub;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ReadSessionCreatorTest {
  EnhancedBigQueryReadStub stub = mock(EnhancedBigQueryReadStub.class);
  BigQueryClient bigQueryClient = mock(BigQueryClient.class);
  UnaryCallable<CreateReadSessionRequest, ReadSession> createReadSessionCall =
      mock(UnaryCallable.class);
  BigQueryReadClient readClient = BigQueryReadClient.create(stub);
  BigQueryClientFactory bigQueryReadClientFactory = mock(BigQueryClientFactory.class);
  TableInfo table =
      TableInfo.newBuilder(
              TableId.of("a", "b"),
              StandardTableDefinition.newBuilder()
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
        new ReadSessionCreatorConfigBuilder().setRequestEncodedBase(encodedBase).build();
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
        new ReadSessionCreatorConfigBuilder().setDefaultParallelism(10).build();
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
}
