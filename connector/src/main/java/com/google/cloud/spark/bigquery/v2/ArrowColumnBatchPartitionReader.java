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

import com.google.cloud.bigquery.connector.common.ArrowUtil;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ArrowSchemaConverter;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

class ArrowColumnBatchPartitionColumnBatchReader implements InputPartitionReader<ColumnarBatch> {
  private static final long maxAllocation = 500 * 1024 * 1024;

  private final ReadRowsHelper readRowsHelper;
  private final ArrowStreamReader reader;
  private final BufferAllocator allocator;
  private final List<String> namesInOrder;
  private ColumnarBatch currentBatch;
  private final BigQueryStorageReadRowsTracer tracer;
  private boolean closed = false;

  static class ReadRowsResponseInputStreamEnumeration
      implements java.util.Enumeration<InputStream> {
    private final Iterator<ReadRowsResponse> responses;
    private ReadRowsResponse currentResponse;
    private final BigQueryStorageReadRowsTracer tracer;

    ReadRowsResponseInputStreamEnumeration(
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

  ArrowColumnBatchPartitionColumnBatchReader(
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString schema,
      ReadRowsHelper readRowsHelper,
      List<String> namesInOrder,
      BigQueryStorageReadRowsTracer tracer) {
    this.allocator =
        ArrowUtil.newRootAllocator(maxAllocation)
            .newChildAllocator("ArrowBinaryIterator", 0, maxAllocation);
    this.readRowsHelper = readRowsHelper;
    this.namesInOrder = namesInOrder;
    this.tracer = tracer;

    InputStream batchStream =
        new SequenceInputStream(
            new ReadRowsResponseInputStreamEnumeration(readRowsResponses, tracer));
    InputStream fullStream = new SequenceInputStream(schema.newInput(), batchStream);

    reader = new ArrowStreamReader(fullStream, allocator, CommonsCompressionFactory.INSTANCE);
  }

  @Override
  public boolean next() throws IOException {
    tracer.nextBatchNeeded();
    if (closed) {
      return false;
    }
    tracer.rowsParseStarted();
    closed = !reader.loadNextBatch();

    if (closed) {
      return false;
    }

    VectorSchemaRoot root = reader.getVectorSchemaRoot();
    if (currentBatch == null) {
      // trying to verify from dev@spark but this object
      // should only need to get created once.  The underlying
      // vectors should stay the same.
      ColumnVector[] columns =
          namesInOrder.stream()
              .map(root::getVector)
              .map(ArrowSchemaConverter::new)
              .toArray(ColumnVector[]::new);

      currentBatch = new ColumnarBatch(columns);
    }
    currentBatch.setNumRows(root.getRowCount());
    tracer.rowsParseFinished(currentBatch.numRows());
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
      tracer.finished();
      readRowsHelper.close();
    } catch (Exception e) {
      throw new IOException("Failure closing stream: " + readRowsHelper, e);
    } finally {
      try {
        AutoCloseables.close(reader, allocator);
      } catch (Exception e) {
        throw new IOException("Failure closing arrow components. stream: " + readRowsHelper, e);
      }
    }
  }
}
