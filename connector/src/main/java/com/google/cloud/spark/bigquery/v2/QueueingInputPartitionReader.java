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
package com.google.cloud.spark.bigquery.v2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/*
 * An {@link InputPartitionReader} that will do round-robin asynchronous reads from
 * other readers.
 *
 * This is useful in a few contexts:
 * * For InputPartitionReaders that have expensive synchronous CPU operations
 *   (e.g. parsing/decompression) this allows for pipelining operations with Spark application level
 *   code (assuming there are free CPU cycles).
 * * A way of composing underlying readers for increased throughput (e.g. if readers are each IO
 *   bound waiting on separate services).
 *
 */
public class QueueingInputPartitionReader implements InputPartitionReader<ColumnarBatch> {

  final static int POLL_TIME = 100;
  private final BlockingQueue<Future<ColumnarBatch>> queue;
  private final List<InputPartitionReader<ColumnarBatch>> readers;
  private final ExecutorService executor;

  // Whether processing of delegates is finished.
  private boolean done = false;
  // Background thread for reading from delegates.
  private Thread readerThread;
  // Holder for exceptions encountered while working with delegates.
  private IOException readException;
  // The more recently loaded batch.
  private Future<ColumnarBatch> currentBatch;

  /**
   * @param readers The readers to read from in a round robin order.
   * @param executor An ExecutorService to process the get method on the delegates. The service will
   * be shutdown when this object is closed.
   */
  QueueingInputPartitionReader(
      List<InputPartitionReader<ColumnarBatch>> readers, ExecutorService executor) {
    this.readers = readers;
    queue = new ArrayBlockingQueue<>(readers.size());
    this.executor = executor;
  }

  @Override
  public boolean next() throws IOException {
    if (readerThread == null) {
      start();
    }

    currentBatch = null;
    while (hasMoreElements() && currentBatch == null) {
      try {
        currentBatch = queue.poll(POLL_TIME, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        break;
      }
    }
    if (readException != null) {
      throw readException;
    }
    if (currentBatch != null) {
      try {
        currentBatch.get();
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new IOException(e);
      }
    }

    return currentBatch != null;
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
    // Tracks which readers have exhausted all of there elements
    boolean[] readersDone = new boolean[readers.size()];
    int readersReady = readers.size();
    Arrays.fill(readersDone, false);
    CountDownLatch[] readerLocks = new CountDownLatch[readers.size()];
    for (int x = 0; x < readerLocks.length; x++) {
      readerLocks[x] = new CountDownLatch(0);
    }

    try {

      while (!done) {
        for (int readerIdx = 0; readerIdx < readers.size(); readerIdx++) {
          if (readersDone[readerIdx]) {
            continue;
          }
          InputPartitionReader<ColumnarBatch> reader = readers.get(readerIdx);
          // Ensure that we don't submit another task for the same reader
          // until the last one completed. This is necessary when some readers run out of
          // tasks.
          readerLocks[readerIdx].await();
          readerLocks[readerIdx] = new CountDownLatch(1);
          readersDone[readerIdx] = !reader.next();

          if (!readersDone[readerIdx]) {

            final int idx = readerIdx;
            queue.put(executor.submit(() -> {
              ColumnarBatch batch = reader.get();
              readerLocks[idx].countDown();
              return batch;
            }));
          } else {
            readersReady--;
            if (readersReady <= 0) {
              done = true;
            }
          }
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
  public ColumnarBatch get() {
    Preconditions.checkState(currentBatch != null);
    try {
      return currentBatch.get();
    } catch (Exception e) {
      // Note we've already called get once above so this
      // shouldn't happen in practice.
      throw new RuntimeException(e);
    }
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
      executor.awaitTermination(POLL_TIME, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // Nothing to do here.
    }

    try {
      AutoCloseables.close(readers);
    } catch (Exception e) {
      throw new RuntimeException("Trouble closing delegate readers", e);
    }
  }
}
