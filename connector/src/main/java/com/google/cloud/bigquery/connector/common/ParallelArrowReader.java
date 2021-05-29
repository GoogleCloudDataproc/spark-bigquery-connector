/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigquery.connector.common;

import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter.Arrow;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A utility class for taking up to N {@link ArrowReader} objects and reading data from them
 * asynchronously. This tries to round robin between all readers given to it to maintain
 * a consistent order. It does not appear spark actually cares about this though
 * so it could be relaxed in the future.
 *
 * This is useful in a few contexts:
 * * For InputPartitionReaders that have expensive synchronous CPU operations
 *   (e.g. parsing/decompression) this allows for pipelining operations with Spark application level
 *   code (assuming there are free CPU cycles).
 * * A way of composing underlying readers for increased throughput (e.g. if readers are each IO
 *   bound waiting on separate services).
 *
 */
public class ParallelArrowReader implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ParallelArrowReader.class);

  // Visible for testing.
  static final int POLL_TIME = 100;
  private final BlockingQueue<Future<ArrowRecordBatch>> queue;
  private final List<ArrowReader> readers;
  private final ExecutorService executor;
  private final VectorLoader loader;
  private final BigQueryStorageReadRowsTracer rootTracer;
  private final BigQueryStorageReadRowsTracer tracers[];

  private volatile Future<ArrowRecordBatch> currentFuture;

  // Whether processing of delegates is finished.
  private volatile boolean done = false;
  // Background thread for reading from delegates.
  private Thread readerThread;
  // Holder for exceptions encountered while working with delegates.
  private volatile IOException readException;

  /**
   * @param readers The readers to read from in a round robin order.
   * @param executor An ExecutorService to process the get method on the delegates. The service will
   *     be shutdown when this object is closed.
   */
  public ParallelArrowReader(
      List<ArrowReader> readers,
      ExecutorService executor,
      VectorLoader loader,
      BigQueryStorageReadRowsTracer tracer) {
    this.readers = readers;
    queue = new ArrayBlockingQueue<>(readers.size());
    this.executor = executor;
    this.loader = loader;
    this.rootTracer = tracer;
    tracers = new BigQueryStorageReadRowsTracer[readers.size()];
    for (int x = 0; x < readers.size(); x++) {
      tracers[x] = rootTracer.forkWithPrefix("reader-thread-" + x);
    }
  }

  ArrowRecordBatch resolveBatch(Future<ArrowRecordBatch> futureBatch)
      throws IOException, InterruptedException {
    try {
      return futureBatch.get();
    } catch (InterruptedException e) {
      log.info("Interrupted while waiting for next batch.");
      ArrowRecordBatch resolvedBatch = null;
      try {
        resolvedBatch = futureBatch.get(10, TimeUnit.SECONDS);
      } catch (Exception se) {
        log.warn("Exception caught when waiting for batch on second try.  Giving up.");
        throw new IOException(se);
      }
      resolvedBatch.close();
      throw e;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new IOException(e);
    }
  }

  public boolean next() throws IOException {
    if (readerThread == null) {
      start();
    }
    rootTracer.nextBatchNeeded();
    rootTracer.readRowsResponseRequested();
    ArrowRecordBatch resolvedBatch = null;
    try {
      while (hasMoreElements() && readException == null && resolvedBatch == null) {
        Future<ArrowRecordBatch> futureBatch = null;
        while (hasMoreElements() && readException == null && futureBatch == null) {
          futureBatch = queue.poll(POLL_TIME, TimeUnit.MILLISECONDS);
        }
        if (futureBatch != null) {
          resolvedBatch = resolveBatch(futureBatch);
        }
      }
    } catch (InterruptedException e) {
      log.info("Interrupted when waiting for next batch.");
      return false;
    }

    if (readException != null) {
      if (resolvedBatch != null) {
        resolvedBatch.close();
      }
      throw readException;
    }
    // We don't have access to bytes here.
    rootTracer.readRowsResponseObtained(/*bytes=*/ 0);

    if (resolvedBatch != null) {
      rootTracer.rowsParseStarted();
      loader.load(resolvedBatch);
      rootTracer.rowsParseFinished(resolvedBatch.getLength());
      resolvedBatch.close();
      return true;
    }
    return false;
  }

  private boolean dataReady(Future<ArrowRecordBatch> currentBatch)
      throws InterruptedException, ExecutionException {
    return currentBatch != null && currentBatch.get() != null;
  }

  private boolean hasMoreElements() {
    return !done || queue.size() > 0;
  }

  private void start() {
    readerThread = new Thread(this::consumeReaders);
    readerThread.setDaemon(true);
    readerThread.start();
    rootTracer.startStream();
  }

  private void consumeReaders() {
    try {
      // Tracks which readers have exhausted all of there elements
      AtomicBoolean[] hasData = new AtomicBoolean[readers.size()];
      long lastBytesRead[] = new long[readers.size()];
      AtomicInteger readersReady = new AtomicInteger(readers.size());
      CountDownLatch[] readerLocks = new CountDownLatch[readers.size()];
      VectorUnloader[] unloader = new VectorUnloader[readers.size()];
      VectorSchemaRoot[] roots = new VectorSchemaRoot[readers.size()];

      for (int x = 0; x < readerLocks.length; x++) {
        readerLocks[x] = new CountDownLatch(0);
        hasData[x] = new AtomicBoolean();
        hasData[x].set(true);
        lastBytesRead[x] = 0;
        roots[x] = readers.get(x).getVectorSchemaRoot();
        unloader[x] =
            new VectorUnloader(roots[x], /*includeNullCount=*/ true, /*alignBuffers=*/ false);
        tracers[x].startStream();
      }

      while (!done) {
        for (int readerIdx = 0; readerIdx < readers.size(); readerIdx++) {
          // Ensure that we don't submit another task for the same reader
          // until the last one completed. This is necessary when some readers run out of
          // tasks.
          readerLocks[readerIdx].await();
          if (!hasData[readerIdx].get()) {
            continue;
          }
          ArrowReader reader = readers.get(readerIdx);
          readerLocks[readerIdx] = new CountDownLatch(1);

          final int idx = readerIdx;
          currentFuture =
              executor.submit(
                  () -> {
                    try {
                      tracers[idx].readRowsResponseRequested();
                      hasData[idx].set(reader.loadNextBatch());
                      long incrementalBytesRead = reader.bytesRead() - lastBytesRead[idx];
                      tracers[idx].readRowsResponseObtained(
                          /*bytesReceived=*/ incrementalBytesRead);
                      lastBytesRead[idx] = reader.bytesRead();
                    } catch (IOException e) {
                      readException = e;
                      hasData[idx].set(false);
                    } catch (Exception e) {
                      readException = new IOException("failed to consume readers", e);
                      hasData[idx].set(false);
                    }
                    ArrowRecordBatch batch = null;
                    if (hasData[idx].get()) {
                      int rows = reader.getVectorSchemaRoot().getRowCount();
                      // Not quite parsing but re-use it here.
                      tracers[idx].rowsParseStarted();
                      batch = unloader[idx].getRecordBatch();
                      tracers[idx].rowsParseFinished(rows);
                    } else {
                      int result = readersReady.addAndGet(-1);
                      if (result <= 0) {
                        done = true;
                      }
                    }
                    readerLocks[idx].countDown();
                    return batch;
                  });
          queue.put(currentFuture);
          currentFuture = null;
        }
      }
    } catch (IOException e) {
      readException = e;
    } catch (InterruptedException e) {
      log.debug("Reader thread interrupted.");
    }
    done = true;
  }

  @Override
  public void close() {
    if (readException != null) {
      log.info("Read exception", readException);
    }
    rootTracer.finished();
    done = true;
    // Try to force reader thread to stop.
    if (readerThread != null) {
      readerThread.interrupt();
      try {
        readerThread.join(10000);
      } catch (InterruptedException e) {
        log.info("Interrupted while waiting for reader thread to finish.");
      }
      if (readerThread.isAlive()) {
        log.warn("Reader thread did not shutdown in 10 seconds.");
      } else {
        log.info("Reader thread stopped.  Queue size: {}", queue.size());
      }
    }
    // Stop any queued tasks from processing.
    executor.shutdownNow();

    try {
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        log.warn("executor did not terminate after 10 seconds");
      }
    } catch (InterruptedException e) {
      log.info("Interrupted when awaiting executor termination");
      // Nothing to do here.
    }

    int inProgress = 0;
    List<Future<ArrowRecordBatch>> leftOverWork = new java.util.ArrayList<>();
    if (currentFuture != null) {
      leftOverWork.add(currentFuture);
    }
    leftOverWork.addAll(queue);
    for (Future<ArrowRecordBatch> batch : leftOverWork) {
      if (batch != null) {
        if (batch.isDone()) {
          try {
            if (batch.get() != null) {
              batch.get().close();
            }
          } catch (Exception e) {
            log.warn("Error closing left over batch", e);
          }
        } else {
          inProgress++;
        }
      }
    }
    if (inProgress > 0) {
      log.warn("Left over tasks in progress {}", inProgress);
    }
    for (BigQueryStorageReadRowsTracer tracer : tracers) {
      tracer.finished();
    }

    for (ArrowReader reader : readers) {
      try {
        // Don't close the stream here because it will consume all of it.
        // We let other components worry about stream closure.
        reader.close(/*close underlying channel*/ false);
      } catch (Exception e) {
        log.info("Trouble closing delegate readers", e);
      }
    }
  }
}
