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
 * Default implementation of {@link PartitionReaderFactory} that falls back to the standard {@link
 * ArrowColumnBatchPartitionReaderContext} for columnar Arrow reads.
 */
public class DefaultPartitionReaderFactory implements PartitionReaderFactory {
  @Override
  public InputPartitionReaderContext<ColumnarBatch> createReaderContext(
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString serializedArrowSchema,
      ReadRowsHelper readRowsHelper,
      List<String> selectedFields,
      BigQueryStorageReadRowsTracer tracer,
      Optional<StructType> userProvidedSchema,
      int numBackgroundThreads,
      ResponseCompressionCodec responseCompressionCodec,
      boolean enableTimestampRebase) {
    return new ArrowColumnBatchPartitionReaderContext(
        readRowsResponses,
        serializedArrowSchema,
        readRowsHelper,
        selectedFields,
        tracer,
        userProvidedSchema,
        numBackgroundThreads,
        responseCompressionCodec,
        enableTimestampRebase);
  }
}
