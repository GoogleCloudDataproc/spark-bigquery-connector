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

import com.google.api.core.ApiFuture;
import com.google.api.core.NanoClock;
import com.google.api.gax.retrying.*;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1.stub.readrows.ApiResultRetryAlgorithm;
import com.google.cloud.bigquery.storage.v1alpha2.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

interface BigQueryDataWriterHelper {

  static BigQueryDataWriterHelper from(
      BigQueryWriteClientFactory writeClientFactory,
      String tablePath,
      ProtoBufProto.ProtoSchema protoSchema,
      RetrySettings createWriteStreamRetrySettings) {
    return new BigQueryDataWriterHelperDefault(
        writeClientFactory, tablePath, protoSchema, createWriteStreamRetrySettings);
  }

  void addRow(ByteString message);

  void finalizeStream() throws IOException;

  String getWriteStreamName();

  long getWriteStreamRowCount();

  void abort();
}

class BigQueryDataWriterHelperDefault implements BigQueryDataWriterHelper {

  final Logger logger = LoggerFactory.getLogger(BigQueryDataWriterHelperDefault.class);

  final long APPEND_REQUEST_SIZE = 1000L * 1000L; // 1MB limit for each append
  static final String OFFSET_ERROR = "Response offset %d did not match expected offset %d.";

  private Executor responseObserverExecutor = MoreExecutors.directExecutor();
  private List<ListenableFuture<Void>> appendResponseValidations = new ArrayList<>();

  private final BigQueryWriteClient writeClient;
  private final String tablePath;
  private final ProtoBufProto.ProtoSchema protoSchema;

  private String writeStreamName;
  private StreamWriter streamWriter;
  private ProtoBufProto.ProtoRows.Builder protoRows;

  private long appendRequestRowCount = 0; // number of rows waiting for the next append request
  private long appendRequestSizeBytes = 0; // number of bytes waiting for the next append request

  private long writeStreamSizeBytes = 0; // total bytes of the current write-stream
  private long writeStreamRowCount = 0; // total offset / rows of the current write-stream

  BigQueryDataWriterHelperDefault(
      BigQueryWriteClientFactory writeClientFactory,
      String tablePath,
      ProtoBufProto.ProtoSchema protoSchema,
      RetrySettings createWriteStreamRetrySettings) {
    this.writeClient = writeClientFactory.createBigQueryWriteClient();
    this.tablePath = tablePath;
    this.protoSchema = protoSchema;

    try {
      this.writeStreamName = createWriteStream(createWriteStreamRetrySettings);
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Could not create write-stream after multiple retries.", e);
    }
    this.streamWriter = createStreamWriter(this.writeStreamName);
    this.protoRows = ProtoBufProto.ProtoRows.newBuilder();
  }

  private String createWriteStream(RetrySettings retrySettings)
      throws ExecutionException, InterruptedException {
    DirectRetryingExecutor<String> directRetryingExecutor =
        new DirectRetryingExecutor<>(
            new RetryAlgorithm<>(
                new ApiResultRetryAlgorithm<>(),
                new ExponentialRetryAlgorithm(retrySettings, NanoClock.getDefaultClock())));
    RetryingFuture<String> retryingFutureWriteStreamName =
        directRetryingExecutor.createFuture(
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
    return directRetryingExecutor.submit(retryingFutureWriteStreamName).get();
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
      appendRequest();
    }

    protoRows.addSerializedRows(message);
    appendRequestSizeBytes += messageSize;
    appendRequestRowCount++;
  }

  private void appendRequest() {
    long offset = writeStreamRowCount;

    Storage.AppendRowsRequest appendRowsRequest =
        createAppendRowsRequest(offset, protoSchema, protoRows, writeStreamName);

    ApiFuture<Storage.AppendRowsResponse> appendRowsResponseApiFuture =
        streamWriter.append(appendRowsRequest);

    appendResponseValidations.add(
        Futures.submit(
            new AppendResponseListener(appendRowsResponseApiFuture, offset),
            responseObserverExecutor));

    clearProtoRows();
    this.writeStreamRowCount += appendRequestRowCount;
    this.writeStreamSizeBytes += appendRequestSizeBytes;
    this.appendRequestRowCount = 0;
    this.appendRequestSizeBytes = 0;
  }

  private Storage.AppendRowsRequest createAppendRowsRequest(
      long offset,
      ProtoBufProto.ProtoSchema protoSchema,
      ProtoBufProto.ProtoRows.Builder protoRows,
      String writeStreamName) {
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
  public void finalizeStream() throws IOException {
    if (this.protoRows.getSerializedRowsCount() != 0) {
      appendRequest();
    }

    awaitAllValidations(appendResponseValidations);

    Storage.FinalizeWriteStreamResponse finalizeResponse =
        writeClient.finalizeWriteStream(
            Storage.FinalizeWriteStreamRequest.newBuilder().setName(writeStreamName).build());

    long expectedFinalizedRowCount = writeStreamRowCount;
    long responseFinalizedRowCount = finalizeResponse.getRowCount();
    if (responseFinalizedRowCount != expectedFinalizedRowCount) {
      throw createOffsetErrorIoException(responseFinalizedRowCount, expectedFinalizedRowCount);
    }

    writeClient.shutdown();

    logger.debug(
        "Write-stream {} finalized with row-count {}",
        writeStreamName,
        finalizeResponse.getRowCount());
  }

  private void awaitAllValidations(List<ListenableFuture<Void>> appendResponseValidations)
      throws IOException {
    Futures.FutureCombiner<Void> allValidations = Futures.whenAllSucceed(appendResponseValidations);
    try {
      allValidations.call(() -> null, responseObserverExecutor).get();
    } catch (InterruptedException e) {
      throw appendInterruptedExceptionToRuntimeException(e);
    } catch (ExecutionException e) {
      throw appendExecutionExceptionToIoException(e);
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
    if (appendResponseValidations != null && !appendResponseValidations.isEmpty()) {
      for (ListenableFuture<Void> listenableFuture : appendResponseValidations) {
        listenableFuture.cancel(true);
      }
    }
    this.protoRows = null;
    this.writeStreamName = null;
    this.responseObserverExecutor = null;
    this.appendResponseValidations = null;
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

  @Override
  public long getWriteStreamRowCount() {
    return writeStreamRowCount;
  }

  private static IOException createOffsetErrorIoException(
      long responseOffset, long expectedOffset) {
    return new IOException(String.format(OFFSET_ERROR, responseOffset, expectedOffset));
  }

  private static RuntimeException appendInterruptedExceptionToRuntimeException(
      InterruptedException e) {
    return new RuntimeException("Interrupted while waiting for append response validations.", e);
  }

  private static IOException appendExecutionExceptionToIoException(ExecutionException e) {
    return new IOException("Failed to append.", e.getCause());
  }

  static class AppendResponseListener implements Callable<Void> {

    final long offset;
    final ApiFuture<Storage.AppendRowsResponse> appendRowsResponseApiFuture;

    AppendResponseListener(
        ApiFuture<Storage.AppendRowsResponse> appendRowsResponseApiFuture, long offset) {
      this.offset = offset;
      this.appendRowsResponseApiFuture = appendRowsResponseApiFuture;
    }

    @Override
    public Void call() throws Exception {
      try {
        Storage.AppendRowsResponse appendRowsResponse = this.appendRowsResponseApiFuture.get();
        if (appendRowsResponse.hasError()) {
          throw new RuntimeException(
              "Append request failed with error: " + appendRowsResponse.getError().getMessage());
        }
        long expectedOffset = this.offset;
        long responseOffset = appendRowsResponse.getOffset();
        if (expectedOffset != responseOffset) {
          throw createOffsetErrorIoException(responseOffset, expectedOffset);
        }
      } catch (InterruptedException e) {
        throw appendInterruptedExceptionToRuntimeException(e);
      } catch (ExecutionException e) {
        throw appendExecutionExceptionToIoException(e);
      }
      return null;
    }
  }
}
