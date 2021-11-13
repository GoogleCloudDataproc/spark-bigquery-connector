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
import com.google.cloud.spark.bigquery.ArrowSchemaConverter;
import com.google.cloud.spark.bigquery.common.GenericArrowColumnBatchPartitionReader;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.*;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

class ArrowColumnBatchPartitionColumnBatchReader implements InputPartitionReader<ColumnarBatch> {

  private GenericArrowColumnBatchPartitionReader columnBatchPartitionReaderHelper;
  private ColumnarBatch currentBatch;
  private boolean closed = false;

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
    this.columnBatchPartitionReaderHelper.getTracer().nextBatchNeeded();
    if (closed) {
      return false;
    }
    this.columnBatchPartitionReaderHelper.getTracer().rowsParseStarted();

    closed = !this.columnBatchPartitionReaderHelper.getReader().loadNextBatch();

    if (closed) {
      return false;
    }

    VectorSchemaRoot root = this.columnBatchPartitionReaderHelper.getReader().root();
    if (currentBatch == null) {
      // trying to verify from dev@spark but this object
      // should only need to get created once.  The underlying
      // vectors should stay the same.
      ColumnVector[] columns =
          this.columnBatchPartitionReaderHelper.getNamesInOrder().stream()
              .map(root::getVector)
              .map(
                  vector ->
                      new ArrowSchemaConverter(
                          vector,
                          this.columnBatchPartitionReaderHelper
                              .getUserProvidedFieldMap()
                              .get(vector.getName())))
              .toArray(ColumnVector[]::new);

      currentBatch = new ColumnarBatch(columns);
    }
    currentBatch.setNumRows(root.getRowCount());
    this.columnBatchPartitionReaderHelper.getTracer().rowsParseFinished(currentBatch.numRows());
    return true;
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    try {
      this.columnBatchPartitionReaderHelper.getTracer().finished();
      this.columnBatchPartitionReaderHelper
          .getCloseables()
          .set(0, this.columnBatchPartitionReaderHelper.getReader());
      this.columnBatchPartitionReaderHelper
          .getCloseables()
          .add(this.columnBatchPartitionReaderHelper.getAllocator());
      AutoCloseables.close(this.columnBatchPartitionReaderHelper.getCloseables());
    } catch (Exception e) {
      throw new IOException(
          "Failure closing arrow components. stream: "
              + this.columnBatchPartitionReaderHelper.getReadRowsHelper(),
          e);
    } finally {
      try {
        this.columnBatchPartitionReaderHelper.getReadRowsHelper().close();
      } catch (Exception e) {
        throw new IOException(
            "Failure closing stream: " + this.columnBatchPartitionReaderHelper.getReadRowsHelper(),
            e);
      }
    }
  }
}
