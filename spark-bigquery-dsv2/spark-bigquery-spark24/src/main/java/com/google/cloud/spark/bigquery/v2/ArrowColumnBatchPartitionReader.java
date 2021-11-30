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

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.common.GenericArrowColumnBatchPartitionReader;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

class ArrowColumnBatchPartitionColumnBatchReader implements InputPartitionReader<ColumnarBatch> {

  private GenericArrowColumnBatchPartitionReader columnBatchPartitionReaderHelper;

  ArrowColumnBatchPartitionColumnBatchReader(
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString schema,
      ReadRowsHelper readRowsHelper,
      List<String> namesInOrder,
      BigQueryStorageReadRowsTracer tracer,
      Optional<StructType> userProvidedSchema,
      int numBackgroundThreads) {
    this.columnBatchPartitionReaderHelper =
        new GenericArrowColumnBatchPartitionReader(
            readRowsResponses,
            schema,
            readRowsHelper,
            namesInOrder,
            tracer,
            userProvidedSchema,
            numBackgroundThreads);
  }

  @Override
  public boolean next() throws IOException {
    return this.columnBatchPartitionReaderHelper.next();
  }

  @Override
  public ColumnarBatch get() {
    return this.columnBatchPartitionReaderHelper.getCurrentBatch();
  }

  @Override
  public void close() throws IOException {
    this.columnBatchPartitionReaderHelper.close();
  }
}
