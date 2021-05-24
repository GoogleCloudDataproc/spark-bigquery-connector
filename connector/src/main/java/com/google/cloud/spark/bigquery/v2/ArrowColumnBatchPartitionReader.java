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
import com.google.cloud.bigquery.connector.common.IteratorMultiplexer;
import com.google.cloud.bigquery.connector.common.ParallelArrowReader;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.ReadRowsResponseInputStreamEnumeration;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ArrowSchemaConverter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

class ArrowColumnBatchPartitionColumnBatchReader implements InputPartitionReader<ColumnarBatch> {
  private static final long maxAllocation = 500 * 1024 * 1024;

  interface ArrowReaderAdapter extends AutoCloseable {
    boolean loadNextBatch() throws IOException;

    VectorSchemaRoot root() throws IOException;
  }

  static class SimpleAdapter implements ArrowReaderAdapter {
    private final ArrowReader reader;

    SimpleAdapter(ArrowReader reader) {
      this.reader = reader;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      return reader.loadNextBatch();
    }

    @Override
    public VectorSchemaRoot root() throws IOException {
      return reader.getVectorSchemaRoot();
    }

    @Override
    public void close() throws Exception {
      reader.close();
    }
  }

  static class ParallelReaderAdapter implements ArrowReaderAdapter {
    private final ParallelArrowReader reader;
    private final VectorLoader loader;
    private final VectorSchemaRoot root;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private IOException initialException;

    ParallelReaderAdapter(
        BufferAllocator allocator,
        List<ArrowReader> readers,
        ExecutorService executor,
        BigQueryStorageReadRowsTracer tracer,
        AutoCloseable closeable) {
      if (closeable != null) {
        closeables.add(closeable);
      }
      Schema schema = null;
      try {
        schema = readers.get(0).getVectorSchemaRoot().getSchema();
      } catch (IOException e) {
        initialException = e;
        closeables.addAll(readers);
      }
      root = VectorSchemaRoot.create(schema, allocator);
      closeables.add(root);
      loader = new VectorLoader(root);
      this.reader = new ParallelArrowReader(readers, executor, loader, tracer);
      closeables.add(reader);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      if (initialException != null) {
        throw new IOException(initialException);
      }
      return reader.next();
    }

    @Override
    public VectorSchemaRoot root() throws IOException {
      return root;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(closeables);
    }
  }

  private final ReadRowsHelper readRowsHelper;
  private final ArrowReaderAdapter reader;
  private final BufferAllocator allocator;
  private final List<String> namesInOrder;
  private ColumnarBatch currentBatch;
  private final BigQueryStorageReadRowsTracer tracer;
  private boolean closed = false;
  private final Map<String, StructField> userProvidedFieldMap;

  ArrowColumnBatchPartitionColumnBatchReader(
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString schema,
      ReadRowsHelper readRowsHelper,
      List<String> namesInOrder,
      BigQueryStorageReadRowsTracer tracer,
      Optional<StructType> userProvidedSchema,
      int numBackgroundThreads) {
    this.allocator =
        ArrowUtil.newRootAllocator(maxAllocation)
            .newChildAllocator("ArrowBinaryIterator", 0, maxAllocation);
    this.readRowsHelper = readRowsHelper;
    this.namesInOrder = namesInOrder;
    this.tracer = tracer;

    List<StructField> userProvidedFieldList =
        Arrays.stream(userProvidedSchema.orElse(new StructType()).fields())
            .collect(Collectors.toList());

    this.userProvidedFieldMap =
        userProvidedFieldList.stream().collect(Collectors.toMap(StructField::name, field -> field));

    // There is a background thread created by ParallelArrowReader that serves
    // as a thread to do parsing on.

    InputStream batchStream =
        new SequenceInputStream(
            new ReadRowsResponseInputStreamEnumeration(readRowsResponses, tracer));
    InputStream fullStream = new SequenceInputStream(schema.newInput(), batchStream);
    if (numBackgroundThreads == 1) {
      reader =
          new ParallelReaderAdapter(
              allocator,
              ImmutableList.of(newArrowStreamReader(fullStream)),
              MoreExecutors.newDirectExecutorService(),
              tracer.forkWithPrefix("BackgroundReader"),
              /*closeable=*/ null);
    } else if (numBackgroundThreads > 1) {
      int threads = numBackgroundThreads - 1;
      ExecutorService backgroundParsingService =
          new ThreadPoolExecutor(
              /*corePoolSize=*/ 1,
              /*maximumPoolSize=*/ threads,
              /*keepAliveTime=*/ 2,
              /*keepAlivetimeUnit=*/ TimeUnit.SECONDS,
              new SynchronousQueue<>(),
              new ThreadPoolExecutor.CallerRunsPolicy());
      IteratorMultiplexer multiplexer =
          new IteratorMultiplexer(readRowsResponses, numBackgroundThreads);
      List<ArrowReader> readers = new ArrayList<>();
      for (int x = 0; x < numBackgroundThreads; x++) {
        InputStream responseStream =
            new SequenceInputStream(
                new ReadRowsResponseInputStreamEnumeration(
                    multiplexer.getSplit(x), tracer.forkWithPrefix("multiplexed-" + x)));
        InputStream schemaAndBatches = new SequenceInputStream(schema.newInput(), responseStream);
        readers.add(newArrowStreamReader(schemaAndBatches));
      }
      reader =
          new ParallelReaderAdapter(
              allocator,
              readers,
              backgroundParsingService,
              tracer.forkWithPrefix("BackgroundReader"),
              multiplexer);
    } else {
      // Zero background threads.
      reader = new SimpleAdapter(newArrowStreamReader(fullStream));
    }
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

    VectorSchemaRoot root = reader.root();
    if (currentBatch == null) {
      // trying to verify from dev@spark but this object
      // should only need to get created once.  The underlying
      // vectors should stay the same.
      ColumnVector[] columns =
          namesInOrder.stream()
              .map(root::getVector)
              .map(
                  vector ->
                      new ArrowSchemaConverter(vector, userProvidedFieldMap.get(vector.getName())))
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

  private ArrowStreamReader newArrowStreamReader(InputStream fullStream) {
    return new ArrowStreamReader(fullStream, allocator, CommonsCompressionFactory.INSTANCE);
  }
}
