package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.Serializable;
import java.util.*;
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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class GenericArrowColumnBatchPartitionReader implements Serializable {
  private static final long maxAllocation = 500 * 1024 * 1024;

  private final ReadRowsHelper readRowsHelper;

  public ArrowReaderAdapter getReader() {
    return reader;
  }

  private final ArrowReaderAdapter reader;
  private final BufferAllocator allocator;
  private final List<String> namesInOrder;
  private final BigQueryStorageReadRowsTracer tracer;
  private final Map<String, StructField> userProvidedFieldMap;

  private final List<AutoCloseable> closeables = new ArrayList<>();

  public ReadRowsHelper getReadRowsHelper() {
    return readRowsHelper;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public List<String> getNamesInOrder() {
    return namesInOrder;
  }

  public BigQueryStorageReadRowsTracer getTracer() {
    return tracer;
  }

  public List<AutoCloseable> getCloseables() {
    return closeables;
  }

  public GenericArrowColumnBatchPartitionReader(
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString schema,
      ReadRowsHelper readRowsHelper,
      List<String> namesInOrder,
      BigQueryStorageReadRowsTracer tracer,
      Optional<StructType> userProvidedSchema,
      int numBackgroundThreads) {
    this.allocator = ArrowUtil.newRootAllocator(maxAllocation);
    this.readRowsHelper = readRowsHelper;
    this.namesInOrder = namesInOrder;
    this.tracer = tracer;
    List<StructField> userProvidedFieldList =
        Arrays.stream(userProvidedSchema.orElse(new StructType()).fields())
            .collect(Collectors.toList());
    this.userProvidedFieldMap =
        userProvidedFieldList.stream().collect(Collectors.toMap(StructField::name, field -> field));
    // place holder for reader.
    closeables.add(null);
    if (numBackgroundThreads == 1) {
      // There is a background thread created by ParallelArrowReader that serves
      // as a thread to do parsing on.
      InputStream fullStream = makeSingleInputStream(readRowsResponses, schema, tracer);
      this.reader =
          new SimpleAdapter.ParallelReaderAdapter(
              allocator,
              ImmutableList.of(newArrowStreamReader(fullStream)),
              MoreExecutors.newDirectExecutorService(),
              tracer.forkWithPrefix("BackgroundReader"),
              /*closeable=*/ null);
    } else if (numBackgroundThreads > 1) {
      // Subtract one because current excess tasks will be executed
      // on round robin thread in ParallelArrowReader.
      ExecutorService backgroundParsingService =
          new ThreadPoolExecutor(
              /*corePoolSize=*/ 1,
              /*maximumPoolSize=*/ numBackgroundThreads - 1,
              /*keepAliveTime=*/ 2,
              /*keepAlivetimeUnit=*/ TimeUnit.SECONDS,
              new SynchronousQueue<>(),
              new ThreadPoolExecutor.CallerRunsPolicy());
      IteratorMultiplexer multiplexer =
          new IteratorMultiplexer(readRowsResponses, numBackgroundThreads);
      List<ArrowReader> readers = new ArrayList<>();
      for (int x = 0; x < numBackgroundThreads; x++) {
        BigQueryStorageReadRowsTracer multiplexedTracer = tracer.forkWithPrefix("multiplexed-" + x);
        InputStream responseStream =
            new SequenceInputStream(
                new ReadRowsResponseInputStreamEnumeration(
                    multiplexer.getSplit(x), multiplexedTracer));
        InputStream schemaAndBatches = new SequenceInputStream(schema.newInput(), responseStream);
        closeables.add(multiplexedTracer::finished);
        readers.add(newArrowStreamReader(schemaAndBatches));
      }
      this.reader =
          new SimpleAdapter.ParallelReaderAdapter(
              allocator,
              readers,
              backgroundParsingService,
              tracer.forkWithPrefix("MultithreadReader"),
              multiplexer);
    } else {
      // Zero background threads.
      InputStream fullStream = makeSingleInputStream(readRowsResponses, schema, tracer);
      this.reader = new SimpleAdapter(newArrowStreamReader(fullStream));
    }
  }

  public Map<String, StructField> getUserProvidedFieldMap() {
    return userProvidedFieldMap;
  }

  public interface ArrowReaderAdapter extends AutoCloseable {
    public boolean loadNextBatch() throws IOException;

    public VectorSchemaRoot root() throws IOException;
  }

  public static class SimpleAdapter implements ArrowReaderAdapter {
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
      // Don't close the stream here since it will be taken care
      // of by closing the ReadRowsHelper below and the way the stream
      // is setup closing it here will cause it to be drained before
      // returning.
      reader.close(/*close stream*/ false);
    }

    public static class ParallelReaderAdapter implements ArrowReaderAdapter {
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
        Schema schema = null;
        closeables.add(closeable);
        try {
          schema = readers.get(0).getVectorSchemaRoot().getSchema();
        } catch (IOException e) {
          initialException = e;
          closeables.addAll(readers);
          this.reader = null;
          this.loader = null;
          this.root = null;
          return;
        }
        BufferAllocator readerAllocator =
            allocator.newChildAllocator("ParallelReaderAllocator", 0, maxAllocation);
        root = VectorSchemaRoot.create(schema, readerAllocator);
        closeables.add(root);
        loader = new VectorLoader(root);
        this.reader = new ParallelArrowReader(readers, executor, loader, tracer);
        closeables.add(0, reader);
        closeables.add(readerAllocator);
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
  }
  // Note this method consumes inputs.
  private InputStream makeSingleInputStream(
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString schema,
      BigQueryStorageReadRowsTracer tracer) {
    InputStream batchStream =
        new SequenceInputStream(
            new ReadRowsResponseInputStreamEnumeration(readRowsResponses, tracer));
    return new SequenceInputStream(schema.newInput(), batchStream);
  }

  private ArrowStreamReader newArrowStreamReader(InputStream fullStream) {
    BufferAllocator childAllocator =
        allocator.newChildAllocator("readerAllocator" + (closeables.size() - 1), 0, maxAllocation);
    closeables.add(childAllocator);
    return new ArrowStreamReader(
        new NonInterruptibleBlockingBytesChannel(fullStream),
        childAllocator,
        CommonsCompressionFactory.INSTANCE);
  }
}
