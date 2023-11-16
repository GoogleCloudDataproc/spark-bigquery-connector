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
import com.google.protobuf.UnknownFieldSet;
import java.io.InputStream;
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
  private boolean didLogResponse = false; // reduce spam

  public ReadRowsResponseInputStreamEnumeration(
      Iterator<ReadRowsResponse> responses, BigQueryStorageReadRowsTracer tracer) {
    this.responses = responses;
    this.tracer = tracer;
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

    // TODO: AQIU: it is hit here!  decompress here?
    if (didLogResponse == false) {
      UnknownFieldSet unknownFieldSet = currentResponse.getUnknownFields();
      java.util.Map<Integer, UnknownFieldSet.Field> unknownFieldSetMap = unknownFieldSet.asMap();
      System.out.printf(
          "AQIU: ReadRowsResponseInputStreamEnumeration ReadRowsResponse UnknownFieldSet.asMap {}"
              + " \n",
          unknownFieldSetMap);
      didLogResponse = true;
    }
    loadNextResponse();
    return ret.getArrowRecordBatch().getSerializedRecordBatch().newInput();
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
