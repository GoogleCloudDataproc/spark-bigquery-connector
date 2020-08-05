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

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1alpha2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BigQueryDataWriterHelper {

  final Logger logger = LoggerFactory.getLogger(BigQueryDataWriterHelper.class);
  final long APPEND_REQUEST_SIZE = 1000L * 1000L; // 1MB limit for each append
  final int WAIT_TIME_FOR_APPEND_RESULTS_MILLIS =
      100; // Number of milliseconds to stand by for append request response validation
  final ExecutorService validateThread = Executors.newCachedThreadPool();

  private final BigQueryWriteClient writeClient;
  private final String tablePath;
  private final ProtoBufProto.ProtoSchema protoSchema;

  private Stream.WriteStream writeStream;
  private StreamWriter streamWriter;
  private ProtoBufProto.ProtoRows.Builder protoRows;

  private long appendRows = 0; // number of rows waiting for the next append request
  private long appendBytes = 0; // number of bytes waiting for the next append request

  private int appendCount = 0;

  private long writeStreamBytes = 0; // total bytes of the current write-stream
  private long writeStreamRows = 0; // total offset / rows of the current write-stream

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
    this.writeStream =
        writeClient.createWriteStream(
            Storage.CreateWriteStreamRequest.newBuilder()
                .setParent(tablePath)
                .setWriteStream(
                    Stream.WriteStream.newBuilder()
                        .setType(Stream.WriteStream.Type.PENDING)
                        .build())
                .build());

    try {
      this.streamWriter = StreamWriter.newBuilder(writeStream.getName()).build();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Could not build stream-writer.", e);
    }
  }

  protected void addRow(ByteString message) throws IOException {
    int messageSize = message.size();

    if (appendBytes + messageSize > APPEND_REQUEST_SIZE) {
      appendRequest();
      appendRows = 0;
      appendBytes = 0;
    }

    protoRows.addSerializedRows(message);
    appendBytes += messageSize;
    appendRows++;
  }

  private void appendRequest() throws IllegalStateException {
    long offset = writeStreamRows;

    Storage.AppendRowsRequest.Builder requestBuilder =
        Storage.AppendRowsRequest.newBuilder().setOffset(Int64Value.of(offset));

    Storage.AppendRowsRequest.ProtoData.Builder dataBuilder =
        Storage.AppendRowsRequest.ProtoData.newBuilder();
    dataBuilder.setWriterSchema(protoSchema);
    dataBuilder.setRows(protoRows.build());

    requestBuilder.setProtoRows(dataBuilder.build()).setWriteStream(writeStream.getName());

    ApiFuture<Storage.AppendRowsResponse> appendRowsResponseApiFuture =
        streamWriter.append(requestBuilder.build());

    validateThread.submit(new AppendResponseObserver(appendRowsResponseApiFuture, offset));

    clearProtoRows();
    this.writeStreamRows += appendRows; // add the # of rows appended to writeStreamRows
    this.writeStreamBytes += appendBytes;
    this.appendCount++;
  }

  protected void finalizeStream() throws IOException {
    if (this.appendRows != 0 || this.appendBytes != 0) {
      appendRequest();
    }

    if (!validateThread.isTerminated()) {
      awaitThread(validateThread);
    }

    Storage.FinalizeWriteStreamResponse finalizeResponse =
        writeClient.finalizeWriteStream(
            Storage.FinalizeWriteStreamRequest.newBuilder().setName(writeStream.getName()).build());

    if (finalizeResponse.getRowCount() != writeStreamRows) {
      throw new IOException("Finalize response had an unexpected row count.");
    }

    writeClient.shutdown();

    logger.debug(
        "Write-stream {} finalized with row-count {}",
        writeStream.getName(),
        finalizeResponse.getRowCount());
  }

  private void awaitThread(ExecutorService validateThread) throws IOException {
    ExponentialBackOff exponentialBackOff =
        new ExponentialBackOff.Builder()
            .setInitialIntervalMillis(WAIT_TIME_FOR_APPEND_RESULTS_MILLIS)
            .setMaxElapsedTimeMillis(appendCount * WAIT_TIME_FOR_APPEND_RESULTS_MILLIS)
            .build();
    while (true) {
      long nextBackOffMillis = exponentialBackOff.nextBackOffMillis();
      if (nextBackOffMillis == ExponentialBackOff.STOP) {
        throw new RuntimeException("Could not validate append rows responses.");
      }
      try {
        validateThread.awaitTermination(
            exponentialBackOff.nextBackOffMillis(), TimeUnit.MILLISECONDS);
        return;
      } catch (InterruptedException ignored) {
      }
    }
  }

  private void clearProtoRows() {
    this.protoRows.clear();
  }

  protected String getWriteStreamName() {
    return writeStream.getName();
  }

  protected long getDataWriterRows() {
    return writeStreamRows;
  }

  protected void abort() {
    if (streamWriter != null) {
      streamWriter.close();
    }
    if (writeClient != null && !writeClient.isShutdown()) {
      writeClient.shutdown();
    }
  }

  static class AppendResponseObserver implements Runnable {

    final long offset;
    final ApiFuture<Storage.AppendRowsResponse> appendRowsResponseApiFuture;

    AppendResponseObserver(
        ApiFuture<Storage.AppendRowsResponse> appendRowsResponseApiFuture, long offset) {
      this.offset = offset;
      this.appendRowsResponseApiFuture = appendRowsResponseApiFuture;
    }

    @Override
    public void run() {
      try {
        Storage.AppendRowsResponse appendRowsResponse = appendRowsResponseApiFuture.get();
        if (appendRowsResponse.hasError()) {
          throw new RuntimeException(
              "Append request failed with error: " + appendRowsResponse.getError().getMessage());
        }
        if (this.offset != this.appendRowsResponseApiFuture.get().getOffset()) {
          throw new RuntimeException("Append request offset did not match expected offset.");
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Could not analyze append response.", e);
      }
    }
  }
}
