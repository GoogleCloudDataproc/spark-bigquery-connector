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
package com.google.cloud.spark.bigquery.v2;

import com.google.api.client.util.Sleeper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.NanoClock;
import com.google.api.gax.retrying.*;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1.stub.readrows.ApiResultRetryAlgorithm;
import com.google.cloud.bigquery.storage.v1alpha2.*;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

interface BigQueryDataWriterHelper {

  static BigQueryDataWriterHelper from(
      BigQueryWriteClientFactory writeClientFactory,
      String tablePath,
      ProtoBufProto.ProtoSchema protoSchema,
      RetrySettings bigqueryDataWriterHelperRetrySettings) {
    return new BigQueryDataWriterHelperDefault(
        writeClientFactory, tablePath, protoSchema, bigqueryDataWriterHelperRetrySettings);
  }

  void addRow(ByteString message);

  long commit() throws IOException;

  String getWriteStreamName();

  void abort();
}

class BigQueryDataWriterHelperDefault implements BigQueryDataWriterHelper {

  final Logger logger = LoggerFactory.getLogger(BigQueryDataWriterHelperDefault.class);

  final long APPEND_REQUEST_SIZE = 1000L * 1000L; // 1MB limit for each append

  private Executor appendRowsResponseListenersExecutor = MoreExecutors.directExecutor();
  private List<SettableFuture<Boolean>> appendRowsResponseCallbackFutures = new ArrayList<>();

  private final BigQueryWriteClient writeClient;
  private final String tablePath;
  private final ProtoBufProto.ProtoSchema protoSchema;
  private final RetrySettings retrySettings;

  private String writeStreamName;
  private StreamWriter streamWriter;
  private ProtoBufProto.ProtoRows.Builder protoRows;

  private long appendRequestRowCount = 0; // number of rows waiting for the next append request
  private long appendRequestSizeBytes = 0; // number of bytes waiting for the next append request

  private long writeStreamSizeBytes = 0; // total bytes of the current write-stream
  private long writeStreamRowCount = 0; // total offset / rows of the current write-stream

  private boolean finalized = false;
  private long finalizedWriteStreamRowCount;

  BigQueryDataWriterHelperDefault(
      BigQueryWriteClientFactory writeClientFactory,
      String tablePath,
      ProtoBufProto.ProtoSchema protoSchema,
      RetrySettings bigqueryDataWriterHelperRetrySettings) {
    this.writeClient = writeClientFactory.createBigQueryWriteClient();
    this.tablePath = tablePath;
    this.protoSchema = protoSchema;
    this.retrySettings = bigqueryDataWriterHelperRetrySettings;

    try {
      this.writeStreamName = retryCreateWriteStream();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Could not create write-stream after multiple retries.", e);
    }
    this.streamWriter = createStreamWriter(this.writeStreamName);
    this.protoRows = ProtoBufProto.ProtoRows.newBuilder();
  }

  private String retryCreateWriteStream() throws ExecutionException, InterruptedException {
    return retryCallable(
        () ->
            this.writeClient
                .createWriteStream(
                    Storage.CreateWriteStreamRequest.newBuilder()
                        .setParent(this.tablePath)
                        .setWriteStream(
                            Stream.WriteStream.newBuilder()
                                .setType(Stream.WriteStream.Type.PENDING)
                                .build())
                        .build())
                .getName());
  }

  private <V> V retryCallable(Callable<V> callable)
      throws ExecutionException, InterruptedException {
    DirectRetryingExecutor<V> directRetryingExecutor =
        new DirectRetryingExecutor<>(
            new RetryAlgorithm<>(
                new ApiResultRetryAlgorithm<>(),
                new ExponentialRetryAlgorithm(this.retrySettings, NanoClock.getDefaultClock())));
    RetryingFuture<V> retryingFuture = directRetryingExecutor.createFuture(callable);
    return directRetryingExecutor.submit(retryingFuture).get();
  }

  private StreamWriter createStreamWriter(String writeStreamName) {
    try {
      return StreamWriter.newBuilder(writeStreamName).build();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Could not build stream-writer.", e);
    }
  }

  @Override
  public void addRow(ByteString message) {
    int messageSize = message.size();

    if (appendRequestSizeBytes + messageSize > APPEND_REQUEST_SIZE) {
      initializeAppendRequest();
    }

    protoRows.addSerializedRows(message);
    appendRequestSizeBytes += messageSize;
    appendRequestRowCount++;
  }

  private void initializeAppendRequest() {
    long offset = writeStreamRowCount;

    Storage.AppendRowsRequest appendRowsRequest =
        createAppendRowsRequest(offset);

    ApiFuture<Storage.AppendRowsResponse> appendRowsResponseApiFuture =
        streamWriter.append(appendRowsRequest);
    SettableFuture<Boolean> callbackFuture = SettableFuture.create();
    ApiFutures.addCallback(appendRowsResponseApiFuture,
            new AppendRowsResponseCallbackValidator(offset, writeStreamName, callbackFuture),
            appendRowsResponseListenersExecutor);
    appendRowsResponseCallbackFutures.add(callbackFuture);

    clearProtoRows();
    this.writeStreamRowCount += appendRequestRowCount;
    this.writeStreamSizeBytes += appendRequestSizeBytes;
    this.appendRequestRowCount = 0;
    this.appendRequestSizeBytes = 0;
  }

  private Storage.AppendRowsRequest createAppendRowsRequest(long offset) {
    Storage.AppendRowsRequest.Builder requestBuilder =
        Storage.AppendRowsRequest.newBuilder().setOffset(Int64Value.of(offset));

    Storage.AppendRowsRequest.ProtoData.Builder dataBuilder =
        Storage.AppendRowsRequest.ProtoData.newBuilder();
    dataBuilder.setWriterSchema(protoSchema);
    dataBuilder.setRows(protoRows.build());

    requestBuilder.setProtoRows(dataBuilder.build()).setWriteStream(writeStreamName);

    return requestBuilder.build();
  }

  @Override
  public long commit() throws IOException {
    if (this.protoRows.getSerializedRowsCount() != 0) {
      initializeAppendRequest(); // TODO: don't do this.
    }

    awaitAllValidations(); // TODO: don't need parameter.

    Storage.FinalizeWriteStreamRequest finalizeWriteStreamRequest =
        Storage.FinalizeWriteStreamRequest.newBuilder().setName(writeStreamName).build();
    Storage.FinalizeWriteStreamResponse finalizeResponse =
        retryFinalizeWriteStream(finalizeWriteStreamRequest);

    long expectedFinalizedRowCount = writeStreamRowCount;
    long responseFinalizedRowCount = finalizeResponse.getRowCount();
    if (responseFinalizedRowCount != expectedFinalizedRowCount) {
      throw new IOException(
          String.format(
              "On stream %s finalization, expected finalized row count %d but received %d",
              writeStreamName, expectedFinalizedRowCount, responseFinalizedRowCount));
    }

    writeClient.shutdown();

    logger.debug(
        "Write-stream {} finalized with row-count {}",
        writeStreamName,
        responseFinalizedRowCount);

    return responseFinalizedRowCount;
  }

  private Storage.FinalizeWriteStreamResponse retryFinalizeWriteStream(
      Storage.FinalizeWriteStreamRequest finalizeWriteStreamRequest) {
    try {
      return retryCallable(() -> writeClient.finalizeWriteStream(finalizeWriteStreamRequest));
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(
          String.format("Could not finalize stream %s.", writeStreamName), e);
    }
  }

  private void awaitAllValidations() {
    ListenableFuture<List<Boolean>> allValidationsSuccessFutures = Futures.successfulAsList(appendRowsResponseCallbackFutures);
    List<Boolean> allValidationsSuccesses = null;
    try {
      allValidationsSuccesses = allValidationsSuccessFutures.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Interrupted while waiting for the combined future of all append response listeners.", e);
    } catch (ExecutionException e) {
      throw new RuntimeException(
          "Failed to retrieve the combined future of all append response listeners", e);
    }
    for(boolean success : allValidationsSuccesses) {
      Preconditions.checkState(success);
    }
    try {
      Sleeper.DEFAULT.sleep(500);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Interrupted while sleeping after validating all append rows responses", e);
    }
  }

  @Override
  public void abort() {
    clearProtoRows();
    if (streamWriter != null) {
      streamWriter.close();
    }
    if (writeClient != null && !writeClient.isShutdown()) {
      writeClient.shutdown();
    }
    this.protoRows = null;
    this.writeStreamName = null;
    this.appendRowsResponseListenersExecutor = null;
    this.appendRowsResponseCallbackFutures = null;
  }

  private void clearProtoRows() {
    if (this.protoRows != null) {
      this.protoRows.clear();
    }
  }

  @Override
  public String getWriteStreamName() {
    return writeStreamName;
  }

  static class AppendRowsResponseCallbackValidator implements ApiFutureCallback<Storage.AppendRowsResponse> {

    final long offset;
    final String writeStreamName;
    final SettableFuture<Boolean> callbackFuture;

    AppendRowsResponseCallbackValidator(long expectedOffset, String writeStreamName, SettableFuture<Boolean> callbackFuture) {
      this.offset = expectedOffset;
      this.writeStreamName = writeStreamName;
      this.callbackFuture = callbackFuture;
    }

    @Override
    public void onFailure(Throwable t) {
      throw new RuntimeException("Could not retrieve AppendRowsResponse.", t);
    }

    @Override
    public void onSuccess(Storage.AppendRowsResponse appendRowsResponse) {
      if (appendRowsResponse.hasError()) {
        throw new UncheckedIOException(
                new IOException(
                        "Append request failed with error: "
                                + appendRowsResponse.getError().getMessage()));
      }
      long expectedOffset = this.offset;
      long responseOffset = appendRowsResponse.getOffset();
      if (expectedOffset != responseOffset) {
        throw new UncheckedIOException(
                new IOException(
                        String.format(
                                "On stream %s append-rows response offset %d did not match expected offset %d.",
                                writeStreamName, responseOffset, expectedOffset)));
      }
      callbackFuture.set(true);
    }
  }
}
