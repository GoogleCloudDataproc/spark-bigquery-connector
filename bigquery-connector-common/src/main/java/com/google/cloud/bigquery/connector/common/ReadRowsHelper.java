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

import static java.util.Objects.requireNonNull;

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadRowsHelper implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ReadRowsHelper.class);
  private final Options options;
  private final Optional<BigQueryStorageReadRowsTracer> bigQueryStorageReadRowsTracer;

  public static final class Options implements Serializable {
    private final int maxReadRowsRetries;
    private final String nullableEndpoint;
    private final int backgroundParsingThreads;
    private final int prebufferResponses;

    public Options(
        int maxReadRowsRetries,
        Optional<String> endpoint,
        int backgroundParsingThreads,
        int prebufferResponses) {
      this.maxReadRowsRetries = maxReadRowsRetries;
      this.nullableEndpoint = endpoint.orElse(null);
      this.backgroundParsingThreads = backgroundParsingThreads;
      this.prebufferResponses = prebufferResponses;
    }

    public int getMaxReadRowsRetries() {
      return maxReadRowsRetries;
    }

    public Optional<String> getEndpoint() {
      return Optional.ofNullable(nullableEndpoint);
    }

    public int numBackgroundThreads() {
      return backgroundParsingThreads;
    }

    public int numPrebufferResponses() {
      return prebufferResponses;
    }
  }

  private final BigQueryClientFactory bigQueryReadClientFactory;
  private final List<ReadRowsRequest.Builder> requests;
  private StreamCombiningIterator incomingStream;

  public ReadRowsHelper(
      BigQueryClientFactory bigQueryReadClientFactory,
      ReadRowsRequest.Builder request,
      Options options,
      Optional<BigQueryStorageReadRowsTracer> bigQueryStorageReadRowsTracer) {
    this.bigQueryReadClientFactory =
        requireNonNull(bigQueryReadClientFactory, "bigQueryReadClientFactory cannot be null");
    this.requests = ImmutableList.of(requireNonNull(request, "request cannot be null"));
    this.options = options;
    this.bigQueryStorageReadRowsTracer = bigQueryStorageReadRowsTracer;
  }

  public ReadRowsHelper(
      BigQueryClientFactory bigQueryReadClientFactory,
      List<ReadRowsRequest.Builder> requests,
      Options options) {
    this.bigQueryReadClientFactory =
        requireNonNull(bigQueryReadClientFactory, "bigQueryReadClientFactory cannot be null");
    this.requests = requireNonNull(requests, "request cannot be null");
    this.options = options;
    this.bigQueryStorageReadRowsTracer = Optional.empty();
  }

  public Iterator<ReadRowsResponse> readRows() {
    if (requests.isEmpty()) {
      // Partition is empty, probably due to dynamic partition pruning. Cannot return
      // StreamCombiningIterator as it
      // requires at least one request
      return Collections.emptyIterator();
    }
    bigQueryStorageReadRowsTracer.ifPresent(tracer -> tracer.startStream());
    BigQueryReadClient client = bigQueryReadClientFactory.getBigQueryReadClient();

    incomingStream =
        new StreamCombiningIterator(
            client, requests, options.prebufferResponses, options.getMaxReadRowsRetries());
    return incomingStream;
  }

  @Override
  public String toString() {
    return requests.toString();
  }

  @Override
  public void close() {
    if (incomingStream != null) {
      try {
        // There appears to be a race when calling cancel for an already
        // consumed stream can cause an exception to be thrown. Since
        // this is part of the shutdown process, it should be safe to
        // ignore the error.
        incomingStream.cancel();
      } catch (Exception e) {
        logger.debug("Error on cancel call", e);
      }
      incomingStream = null;
    }
  }
}
