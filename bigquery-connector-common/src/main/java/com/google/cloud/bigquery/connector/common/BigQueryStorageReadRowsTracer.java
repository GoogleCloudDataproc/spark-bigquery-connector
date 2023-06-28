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

import java.io.Serializable;

/**
 * Interface to capture tracing in information for the BigQuery connector. Modelled after {@link
 * com.google.api.gax.tracing.ApiTracer}
 *
 * <p>A single instance of a tracer corresponds with a single logical stream from the service.
 *
 * <p>Paired start/end methods are expected to be called from the same thread.
 *
 * <p>For internal use only.
 */
public interface BigQueryStorageReadRowsTracer extends Serializable {
  /** Record stream initialization time. */
  void startStream();
  /** Indicates a fully decoded element has been requested by spark (i.e. Arrow RecordBatch). */
  void rowsParseStarted();

  /** Indicates when a decoded item was delivered. */
  void rowsParseFinished(long rowsParsed);

  /** Indicates the next ReadRowsResponse was requested from the server. */
  void readRowsResponseRequested();

  /** Indicates the next ReadRowsResponse was requested from the server. */
  void readRowsResponseObtained(long bytesReceived);

  /** The ReadRows stream has finished. */
  void finished();

  /** Called when the next batch is needed from spark. */
  void nextBatchNeeded();

  /**
   * Must only be called before any calls are made to the tracer. This is intended for cases when
   * multiple threads might be used for processing one stream. tracer that is distinguished between
   * IDs.
   *
   * @param id A distinguisher to use.
   * @return A new tracer with the ID>
   */
  BigQueryStorageReadRowsTracer forkWithPrefix(String id);

  long getBytesRead();

  long getRowsRead();
}
