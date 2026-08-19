/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.spark.bigquery.v2.context;

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A pluggable factory interface that allows customizing the creation of {@link
 * InputPartitionReaderContext} for columnar Arrow reads.
 */
public interface PartitionReaderFactory {
  /**
   * Creates a custom reader context for reading columnar batches.
   *
   * @param readRowsResponses iterator of the BQ storage API read rows responses
   * @param serializedArrowSchema the serialized Arrow schema from the read session
   * @param readRowsHelper helper tool to fetch responses from streams
   * @param selectedFields list of selected field names
   * @param tracer execution path tracer for storage reads
   * @param userProvidedSchema optional schema override provided by the user
   * @param numBackgroundThreads number of background threads to allocate for reading
   * @param responseCompressionCodec compression codec type used by the stream
   * @param enableTimestampRebase flag to control rebase behavior for timestamps
   * @return custom input partition reader context
   */
  InputPartitionReaderContext<ColumnarBatch> createReaderContext(
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString serializedArrowSchema,
      ReadRowsHelper readRowsHelper,
      List<String> selectedFields,
      BigQueryStorageReadRowsTracer tracer,
      Optional<StructType> userProvidedSchema,
      int numBackgroundThreads,
      ResponseCompressionCodec responseCompressionCodec,
      boolean enableTimestampRebase);

  /** Holder class to hold the mutable static instance. */
  class Holder {
    private static final PartitionReaderFactory DEFAULT = new DefaultPartitionReaderFactory();
    private static volatile PartitionReaderFactory instance = DEFAULT;
  }

  /**
   * Gets the currently active factory.
   *
   * @return the active factory
   */
  static PartitionReaderFactory get() {
    return Holder.instance;
  }

  /**
   * Registers a pluggable factory. Passing {@code null} resets to the default factory.
   *
   * @param factory the factory to register, or {@code null} to reset to default
   */
  static void register(PartitionReaderFactory factory) {
    Holder.instance = (factory != null) ? factory : Holder.DEFAULT;
  }
}
