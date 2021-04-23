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
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A utility class for taking up to N {@link ArrowReader} objects and reading data from them
 * asynchronously.
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
  final static int POLL_TIME = 100;
  private final BlockingQueue<Future<ArrowRecordBatch>> queue;
  private final List<ArrowReader> readers;
  private final ExecutorService executor;
  private final VectorLoader loader;

  // Whether processing of delegates is finished.
  private volatile boolean done = false;
  // Background thread for reading from delegates.
  private Thread readerThread;
  // Holder for exceptions encountered while working with delegates.
  private volatile IOException readException;

  /**
   * @param readers The readers to read from in a round robin order.
   * @param executor An ExecutorService to process the get method on the delegates. The service will
   * be shutdown when this object is closed.
   */
  public ParallelArrowReader(
      List<ArrowReader> readers, ExecutorService executor,
      VectorLoader loader) {
    this.readers = readers;
    queue = new ArrayBlockingQueue<>(readers.size());
    this.executor = executor;
    this.loader = loader;
  }

  public boolean next() throws IOException {
    if (readerThread == null) {
      start();
    }

    Future<ArrowRecordBatch> currentBatch = null;
    try {
      while (hasMoreElements() && !dataReady(currentBatch) && readException == null) {
        try {
          currentBatch = queue.poll(POLL_TIME, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          break;
        }
      }
      if (readException != null) {
        throw readException;
      }

      if (dataReady(currentBatch)) {
        loader.load(currentBatch.get());
        currentBatch.get().close();
        return true;
      }
      return false;
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new IOException(e);
    }
  }

  private boolean dataReady(Future<ArrowRecordBatch> currentBatch)
      throws InterruptedException, ExecutionException {
    return currentBatch != null
        && currentBatch.get() != null;
  }

  private boolean hasMoreElements() {
    return !done || queue.size() > 0;
  }

  private void start() {
    readerThread = new Thread(this::consumeReaders);
    readerThread.setDaemon(true);
    readerThread.start();
  }

  private void consumeReaders() {
    try {
      // Tracks which readers have exhausted all of there elements
      AtomicBoolean[] hasData = new AtomicBoolean[readers.size()];
      AtomicInteger readersReady = new AtomicInteger(readers.size());
      CountDownLatch[] readerLocks = new CountDownLatch[readers.size()];
      VectorUnloader[] unloader = new VectorUnloader[readers.size()];
      VectorSchemaRoot[] roots = new VectorSchemaRoot[readers.size()];
      for (int x = 0; x < readerLocks.length; x++) {
        readerLocks[x] = new CountDownLatch(0);
        hasData[x] = new AtomicBoolean();
        hasData[x].set(true);
        roots[x] = readers.get(x).getVectorSchemaRoot();
        unloader[x] = new VectorUnloader(roots[x],
            /*includeNullCount=*/true, /*alignBuffers=*/false);
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
          queue.put(executor.submit(() -> {
            try {
              hasData[idx].set(reader.loadNextBatch());
            } catch (IOException e) {
              readException = e;
              return null;
            } catch (Exception e) {
              readException = new IOException(e);
              return null;
            }
            ArrowRecordBatch batch = null;
            if (hasData[idx].get()) {
              batch = unloader[idx].getRecordBatch();
            } else {
              int result = readersReady.addAndGet(-1);
              if (result <= 0) {
                done = true;
              }
            }
            readerLocks[idx].countDown();
            return batch;
          }));

        }
      }
    } catch (IOException e) {
      readException = e;
    } catch (InterruptedException e) {
      // This should only happen on shutdown.
    }
    done = true;
  }

  @Override
  public void close() {
    done = true;
    // Try to force reader thread to stop.
    if (readerThread != null) {
      readerThread.interrupt();
    }
    // Stop any queued tasks from processing.
    executor.shutdownNow();

    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // Nothing to do here.
    }

    while (!queue.isEmpty()) {
      Future<ArrowRecordBatch> batch = queue.poll();
      try {
        if (batch != null) {
          if (batch.isDone() && batch.get() != null) {
            batch.get().close();
          }
        }
      } catch (Exception e) {
        log.info("Error closing left over batch", e);
      }
    }

    try {
      AutoCloseables.close(readers);
    } catch (Exception e) {
      throw new RuntimeException("Trouble closing delegate readers", e);
    }
  }
}
