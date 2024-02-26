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
package com.google.cloud.bigquery.connector.common;

import com.google.api.client.util.Sleeper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.NanoClock;
import com.google.api.gax.retrying.*;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.Exceptions.OffsetAlreadyExists;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.bigquery.storage.v1.stub.readrows.ApiResultRetryAlgorithm;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class which sketches out the necessary functions in order for a Spark DataWriter to interact
 * with the BigQuery Storage Write API.
 */
public class BigQueryDirectDataWriterHelper {
  final Logger logger = LoggerFactory.getLogger(BigQueryDirectDataWriterHelper.class);

  // multiplying with 0.95 so as to keep a buffer preventing the quota limits
  final long MAX_APPEND_ROWS_REQUEST_SIZE = (long) (StreamWriter.getApiMaxRequestBytes() * 0.95);

  private final BigQueryWriteClient writeClient;
  private final String tablePath;
  private final ProtoSchema protoSchema;
  private final RetrySettings retrySettings;
  private final Optional<String> traceId;
  private final int partitionId;
  private final boolean writeAtLeastOnce;

  private String writeStreamName;
  private StreamWriter streamWriter;
  private ProtoRows.Builder protoRows;

  private long appendRequestRowCount = 0; // number of rows waiting for the next append request
  private long appendRequestSizeBytes = 0; // number of bytes waiting for the next append request
  private long writeStreamRowCount = 0; // total offset / rows of the current write-stream

  private final ExecutorService appendRowsExecutor = Executors.newSingleThreadExecutor();
  private final Queue<ApiFuture<AppendRowsResponse>> appendRowsFuturesQueue = new LinkedList<>();

  public BigQueryDirectDataWriterHelper(
      BigQueryClientFactory writeClientFactory,
      String tablePath,
      ProtoSchema protoSchema,
      RetrySettings bigqueryDataWriterHelperRetrySettings,
      Optional<String> traceId,
      int partitionId,
      boolean writeAtLeastOnce) {
    this.writeClient = writeClientFactory.getBigQueryWriteClient();
    this.tablePath = tablePath;
    this.protoSchema = protoSchema;
    this.retrySettings = bigqueryDataWriterHelperRetrySettings;
    this.traceId = traceId;
    this.partitionId = partitionId;
    this.writeAtLeastOnce = writeAtLeastOnce;

    if (writeAtLeastOnce) {
      this.writeStreamName = this.tablePath + "/_default";
    } else {
      try {
        this.writeStreamName = retryCreateWriteStream();
      } catch (ExecutionException | InterruptedException e) {
        throw new BigQueryConnectorException(
            "Could not create write-stream after multiple retries", e);
      }
    }
    this.streamWriter = createStreamWriter(this.writeStreamName);
    this.protoRows = ProtoRows.newBuilder();
  }

  /**
   * Submits a callable that creates a BigQuery Storage Write API write-stream to function
   * {retryCallable}.
   *
   * @see this#retryCallable(Callable createWriteStream)
   * @return The write-stream name, if it was successfully created.
   * @throws ExecutionException If retryCallable failed to create the write-stream after multiple
   *     retries.
   * @throws InterruptedException If retryCallable was interrupted while creating the write-stream
   *     during a retry.
   */
  private String retryCreateWriteStream() throws ExecutionException, InterruptedException {
    return retryCallable(
        () ->
            this.writeClient
                .createWriteStream(
                    CreateWriteStreamRequest.newBuilder()
                        .setParent(this.tablePath)
                        .setWriteStream(
                            WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build())
                        .build())
                .getName());
  }

  /**
   * A helper method in order to retry certain tasks: currently used for creating a write-stream,
   * and finalizing a write-stream, if those requests reached a retriable error.
   *
   * @param callable The callable to retry.
   * @param <V> The return value of the callable (currently, either a String (write-stream name) or
   *     a FinalizeWriteStreamResponse.
   * @return V.
   * @throws ExecutionException If retryCallable failed to create the write-stream after multiple
   *     retries.
   * @throws InterruptedException If retryCallable was interrupted while creating the write-stream
   *     during a retry.
   */
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
      StreamWriter.Builder streamWriter =
          StreamWriter.newBuilder(writeStreamName, writeClient)
              .setWriterSchema(this.protoSchema)
              .setEnableConnectionPool(this.writeAtLeastOnce)
              .setRetrySettings(this.retrySettings);
      if (traceId.isPresent()) {
        streamWriter.setTraceId(traceId.get());
      }
      return streamWriter.build();
    } catch (IOException e) {
      throw new BigQueryConnectorException("Could not build stream-writer", e);
    }
  }

  /**
   * Adds a row to the protoRows, which acts as a buffer; but before, checks if the current message
   * size in bytes will cause the protoRows buffer to exceed the maximum APPEND_REQUEST_SIZE, and if
   * it will, sends an append rows request first.
   *
   * @see this#sendAppendRowsRequest()
   * @param message The row, in a ByteString message, to be added to protoRows.
   * @throws IOException If sendAppendRowsRequest fails.
   */
  public void addRow(ByteString message) throws IOException {
    int messageSize = message.size() + 2; // Protobuf overhead is at least 2 bytes per row.

    if (appendRequestSizeBytes + messageSize > MAX_APPEND_ROWS_REQUEST_SIZE) {
      // If a single row exceeds the maximum size for an append request, this is a nonrecoverable
      // error.
      if (messageSize > MAX_APPEND_ROWS_REQUEST_SIZE) {
        throw new IOException(
            String.format(
                "A single row of size %d bytes exceeded the maximum of %d bytes for an"
                    + " append-rows-request size",
                messageSize, MAX_APPEND_ROWS_REQUEST_SIZE));
      }
      sendAppendRowsRequest();
    }

    protoRows.addSerializedRows(message);
    appendRequestSizeBytes += messageSize;
    appendRequestRowCount++;
  }

  /**
   * Throws an exception if there is an error in any of the responses received thus far. Since
   * responses arrive in order, we proceed to check the next response only after the previous
   * response has arrived.
   *
   * @throws BigQueryConnectorException If there was an invalid response
   */
  private void checkForFailedResponse(boolean waitForResponse) {
    ApiFuture<AppendRowsResponse> validatedAppendRowsFuture = null;
    while ((validatedAppendRowsFuture = appendRowsFuturesQueue.peek()) != null) {
      if (waitForResponse || validatedAppendRowsFuture.isDone()) {
        appendRowsFuturesQueue.poll();
        boolean succeeded = false;
        try {
          AppendRowsResponse response = validatedAppendRowsFuture.get();
          // If we got here, the response was successful
          succeeded = true;
        } catch (ExecutionException e) {
          if (e.getCause().getClass() == OffsetAlreadyExists.class) {
            // Under heavy load the WriteAPI client library may resend a request due to failed
            // connection. The OffsetAlreadyExists exception indicates a request was previously
            // sent, hence it is safe to ignore it.
            logger.warn("Ignoring OffsetAlreadyExists exception: {}", e.getCause().getMessage());
            succeeded = true;
          } else {
            logger.error(
                "Write-stream {} with name {} exception while retrieving AppendRowsResponse",
                partitionId,
                writeStreamName);
            throw new BigQueryConnectorException(
                "Execution Exception while retrieving AppendRowsResponse", e);
          }
        } catch (InterruptedException e) {
          logger.error(
              "Write-stream {} with name {} interrupted exception while retrieving AppendRowsResponse",
              partitionId,
              writeStreamName);
          throw new BigQueryConnectorException(
              "Interrupted Exception while retrieving AppendRowsResponse", e);
        } finally {
          if (!succeeded) {
            appendRowsExecutor.shutdown();
          }
        }
      } else {
        break;
      }
    }
  }

  /**
   * Sends an AppendRowsRequest to the BigQuery Storage Write API.
   *
   * @throws IOException If the append rows request fails: either by returning the wrong offset
   *     (deduplication error) or if the response contains an error.
   */
  private void sendAppendRowsRequest() throws IOException {
    checkForFailedResponse(false /*waitForResponse*/);
    long offset = this.writeAtLeastOnce ? -1 : writeStreamRowCount;

    ApiFuture<AppendRowsResponse> appendRowsResponseApiFuture =
        streamWriter.append(protoRows.build(), offset);
    ApiFuture<AppendRowsResponse> validatedAppendRowsFuture =
        ApiFutures.transformAsync(
            appendRowsResponseApiFuture,
            appendRowsResponse -> validateAppendRowsResponse(appendRowsResponse, offset),
            appendRowsExecutor);
    appendRowsFuturesQueue.add(validatedAppendRowsFuture);
    clearProtoRows();
    this.writeStreamRowCount += appendRequestRowCount;
    this.appendRequestRowCount = 0;
    this.appendRequestSizeBytes = 0;
  }

  /**
   * Validates an AppendRowsResponse, after retrieving its future: makes sure the responses' future
   * matches the expectedOffset, and returned with no errors.
   *
   * @param appendRowsResponse The AppendRowsResponse
   * @param expectedOffset The expected offset to be returned by the response.
   * @throws IOException If the response returned with error, or the offset did not match the
   *     expected offset.
   */
  private ApiFuture<AppendRowsResponse> validateAppendRowsResponse(
      AppendRowsResponse appendRowsResponse, long expectedOffset) throws IOException {
    if (appendRowsResponse.hasError()) {
      throw new IOException(
          "Append request failed with error: " + appendRowsResponse.getError().getMessage());
    }

    if (!this.writeAtLeastOnce) {
      AppendRowsResponse.AppendResult appendResult = appendRowsResponse.getAppendResult();
      long responseOffset = appendResult.getOffset().getValue();

      if (expectedOffset != responseOffset) {
        throw new IOException(
            String.format(
                "On stream %s append-rows response, offset %d did not match expected offset %d",
                writeStreamName, responseOffset, expectedOffset));
      }
    }
    return ApiFutures.immediateFuture(appendRowsResponse);
  }

  /**
   * Appends any data that remains in the protoRows, waits for 500 milliseconds, and finalizes the
   * write-stream. This also closes the internal StreamWriter, so that the helper instance is not
   * usable after calling <code>finalizeStream()</code>.
   *
   * @return The finalized row-count of the write-stream.
   * @throws IOException If the row-count returned by the FinalizeWriteStreamResponse does not match
   *     the expected offset (which is equal to the number of rows appended thus far).
   * @see this#writeStreamRowCount
   */
  public long finalizeStream() throws IOException {
    if (this.protoRows.getSerializedRowsCount() != 0) {
      sendAppendRowsRequest();
    }
    try {
      checkForFailedResponse(true /* waitForResponse */);
    } finally {
      appendRowsExecutor.shutdown();
    }
    long responseFinalizedRowCount = writeStreamRowCount;

    if (!this.writeAtLeastOnce) {
      waitBeforeFinalization();

      FinalizeWriteStreamRequest finalizeWriteStreamRequest =
          FinalizeWriteStreamRequest.newBuilder().setName(writeStreamName).build();
      FinalizeWriteStreamResponse finalizeResponse =
          retryFinalizeWriteStream(finalizeWriteStreamRequest);

      long expectedFinalizedRowCount = writeStreamRowCount;
      responseFinalizedRowCount = finalizeResponse.getRowCount();
      if (responseFinalizedRowCount != expectedFinalizedRowCount) {
        throw new IOException(
            String.format(
                "On stream %s finalization, expected finalized row count %d but received %d",
                writeStreamName, expectedFinalizedRowCount, responseFinalizedRowCount));
      }
    }

    logger.debug(
        "Write-stream {} with name {} finalized with row-count {}",
        partitionId,
        writeStreamName,
        responseFinalizedRowCount);

    clean();

    return responseFinalizedRowCount;
  }

  /**
   * A helper method in order to submit for retry (using method retryCallable) a callable that
   * finalizes the write-stream; useful if finalization encountered a retriable error.
   *
   * @param finalizeWriteStreamRequest The request to send to the writeClient in order to finalize
   *     the write-stream.
   * @return The FinalizeWriteStreamResponse
   * @see com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse
   */
  private FinalizeWriteStreamResponse retryFinalizeWriteStream(
      FinalizeWriteStreamRequest finalizeWriteStreamRequest) {
    try {
      return retryCallable(() -> writeClient.finalizeWriteStream(finalizeWriteStreamRequest));
    } catch (ExecutionException | InterruptedException e) {
      throw new BigQueryConnectorException(
          String.format("Could not finalize stream %s.", writeStreamName), e);
    }
  }

  /** Waits 500 milliseconds. In order to be used as a cushioning period before finalization. */
  private void waitBeforeFinalization() {
    try {
      Sleeper.DEFAULT.sleep(500);
    } catch (InterruptedException e) {
      throw new BigQueryConnectorException(
          String.format(
              "Interrupted while sleeping before finalizing write-stream %s", writeStreamName),
          e);
    }
  }

  /**
   * Deletes the data left over in the protoRows, using method clearProtoRows, closes the
   * StreamWriter, shuts down the WriteClient, and nulls out the protoRows and write-stream-name.
   * This also closes the internal StreamWriter, so that the helper instance is not * usable after
   * calling <code>commit()</code>.
   */
  public void abort() {
    clean();
    this.protoRows = null;
    this.writeStreamName = null;
  }

  private void clean() {
    clearProtoRows();
    if (streamWriter != null) {
      streamWriter.close();
    }
  }

  private void clearProtoRows() {
    if (this.protoRows != null) {
      this.protoRows.clear();
    }
  }

  public String getWriteStreamName() {
    return writeStreamName;
  }
}
