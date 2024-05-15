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

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Instrumented {@link java.util.Enumeration} for translating bytes received from the BQ Storage API
 * to continuous input streams.
 */
public class ReadRowsResponseInputStreamEnumeration implements java.util.Enumeration<InputStream> {

  private final Iterator<ReadRowsResponse> responses;
  private ReadRowsResponse currentResponse;
  private final BigQueryStorageReadRowsTracer tracer;
  private final ResponseCompressionCodec responseCompressionCodec;

  public ReadRowsResponseInputStreamEnumeration(
      Iterator<ReadRowsResponse> responses,
      BigQueryStorageReadRowsTracer tracer,
      ResponseCompressionCodec compressionCodec) {
    this.responses = responses;
    this.tracer = tracer;
    this.responseCompressionCodec = compressionCodec;
    loadNextResponse();
  }

  public boolean hasMoreElements() {
    return currentResponse != null;
  }

  public InputStream nextElement() {
    if (!hasMoreElements()) {
      throw new NoSuchElementException("No more responses");
    }
    ReadRowsResponse ret = currentResponse;
    loadNextResponse();
    try {
      return new ByteArrayInputStream(DecompressReadRowsResponse.decompressArrowRecordBatch(
          ret, this.responseCompressionCodec));
    } catch (IOException e) {
      throw new UncheckedIOException("Could not read rows", e);
    }
  }

  void loadNextResponse() {
    // hasNext is actually the blocking call, so call  readRowsResponseRequested
    // here.
    tracer.readRowsResponseRequested();
    if (responses.hasNext()) {
      currentResponse = responses.next();
    } else {
      currentResponse = null;
    }
    tracer.readRowsResponseObtained(
        currentResponse == null
            ? 0
            : currentResponse.getArrowRecordBatch().getSerializedRecordBatch().size());
  }
}
