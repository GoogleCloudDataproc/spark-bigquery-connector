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
import com.google.api.core.ApiFutureToListenableFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1alpha2.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class BigQueryDataWriterHelper {

  final Logger logger = LoggerFactory.getLogger(BigQueryDataWriterHelper.class);
  final long APPEND_REQUEST_SIZE = 1000L * 1000L; // 1MB limit for each append
  final int INITIAL_BACKOFF_MILLIS = 100;
  final int MAX_ELAPSED_TIME_MILLIS = 60000 * 5; // 3 minute max waiting time.
  final Executor responseObserverExecutor = MoreExecutors.directExecutor();
  final List<ListenableFuture<Void>> appendResponseValidations = new ArrayList<>();

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

  private boolean awaitingAppendValidations = true;

  protected BigQueryDataWriterHelper(
      BigQueryWriteClientFactory writeClientFactory,
      String tablePath,
      ProtoBufProto.ProtoSchema protoSchema) {
    this.writeClient = writeClientFactory.createBigQueryWriteClient();
    this.tablePath = tablePath;
    this.protoSchema = protoSchema;

    createWriteStreamAndStreamWriter();
    this.protoRows = ProtoBufProto.ProtoRows.newBuilder();
  }

  private void createWriteStreamAndStreamWriter() {
    this.writeStreamName =
        writeClient
            .createWriteStream(
                Storage.CreateWriteStreamRequest.newBuilder()
                    .setParent(tablePath)
                    .setWriteStream(
                        Stream.WriteStream.newBuilder()
                            .setType(Stream.WriteStream.Type.PENDING)
                            .build())
                    .build())
            .getName();
    try {
      this.streamWriter = StreamWriter.newBuilder(this.writeStreamName).build();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Could not build stream-writer.", e);
    }
  }

  protected void addRow(ByteString message) {
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

  protected void finalizeStream() throws IOException {
    if (this.appendRequestRowCount != 0 || this.appendRequestSizeBytes != 0) {
      appendRequest();
    }

    awaitAllValidations(appendResponseValidations);

    Storage.FinalizeWriteStreamResponse finalizeResponse =
        writeClient.finalizeWriteStream(
            Storage.FinalizeWriteStreamRequest.newBuilder().setName(writeStreamName).build());

    long expectedFinalizedRowCount = writeStreamRowCount;
    if (finalizeResponse.getRowCount() != expectedFinalizedRowCount) {
      throw new IOException(
          String.format(
              "Finalize response row count %d did not match expected row count %d.",
              finalizeResponse.getRowCount(), expectedFinalizedRowCount));
    }

    writeClient.shutdown();

    logger.debug(
        "Write-stream {} finalized with row-count {}",
        writeStreamName,
        finalizeResponse.getRowCount());
  }

  private void awaitAllValidations(List<ListenableFuture<Void>> appendResponseValidations) {
    Futures.FutureCombiner<Void> allValidations = Futures.whenAllSucceed(appendResponseValidations);
    try {
      allValidations.call(() -> null, responseObserverExecutor).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Interrupted while waiting for append response validations.", e);
    }
  }

  private void clearProtoRows() {
    this.protoRows.clear();
  }

  protected String getWriteStreamName() {
    return writeStreamName;
  }

  protected long getWriteStreamRowCount() {
    return writeStreamRowCount;
  }

  protected void abort() {
    if (streamWriter != null) {
      streamWriter.close();
    }
    if (writeClient != null && !writeClient.isShutdown()) {
      writeClient.shutdown();
    }
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
        long expectedOffset = this.offset;
        if (appendRowsResponse.hasError()) {
          throw new RuntimeException(
              "Append request failed with error: " + appendRowsResponse.getError().getMessage());
        }
        if (expectedOffset != appendRowsResponse.getOffset()) {
          throw new RuntimeException(
              String.format(
                  "Append response offset %d did not match expected offset %d.",
                  appendRowsResponse.getOffset(), expectedOffset));
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Could not analyze append response.", e);
      }
      return null;
    }
  }
}
