package com.google.cloud.bigquery.connector.common;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages reading ahead from an iterator and dividing it across multiple iterators that can be read
 * in a round-robin fashion.
 *
 * <p>Useful to parallelizing work from an iterator where order must still be maintained.
 *
 * @param <T> Type of iterable object.
 */
public class IteratorMultiplexer<T> implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(IteratorMultiplexer.class);
  private final Iterator<T> iterator;
  private final int splits;
  private final QueueIterator<T>[] iterators;
  private Thread worker;
  private volatile RuntimeException rethrow;

  /**
   * Construct a new instance.
   *
   * @param iterator The Iterator to read from.
   * @param splits The number of output iterators that will read from iterator.
   */
  public IteratorMultiplexer(Iterator<T> iterator, int splits) {
    this.iterator = iterator;
    this.splits = splits;

    // Filled in when initializing iterators.
    iterators = new QueueIterator[splits];
    for (int x = 0; x < splits; x++) {
      iterators[x] = new QueueIterator<>();
    }
  }

  @Override
  public void close() {
    if (worker != null) {
      worker.interrupt();
      try {
        worker.join(/*millis=*/ 1000);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting on worker thread shutdown.", e);
      }
      worker = null;
      if (rethrow != null) {
        log.info("Error occurred while closing.", rethrow);
      }
    } else {
      for (int x = 0; x < splits; x++) {
        iterators[x].done.set(true);
      }
    }
  }

  void readAhead() {
    try {
      boolean hasMore = true;
      while (hasMore) {
        for (int x = 0; x < splits; x++) {
          if (iterator.hasNext()) {
            T value = iterator.next();
            iterators[x].queue.put(value);
          } else {
            hasMore = false;
            break;
          }
        }
      }
    } catch (InterruptedException e) {
      log.info("Worker thread had error. Ending all iterators");
      rethrow = new RuntimeException("Worker thread interrupted");
    } catch (RuntimeException e) {
      rethrow = e;
    }
    for (int x = 0; x < splits; x++) {
      iterators[x].done.set(true);
    }
  }

  public Iterator<T> getSplit(int split) {
    if (worker == null) {
      worker = new Thread(this::readAhead, "readahead-worker");
      worker.setDaemon(true);
      worker.start();
    }
    return iterators[split];
  }

  private class QueueIterator<T> implements Iterator<T> {
    private final ArrayBlockingQueue<T> queue = new ArrayBlockingQueue<>(/*capacity=*/ 1);
    private final AtomicBoolean done = new AtomicBoolean(false);

    private T t = null;

    @Override
    public boolean hasNext() {
      if (!mightHaveNext()) {
        return false;
      }
      t = null;
      try {
        while (t == null && mightHaveNext()) {
          t = queue.poll(10, TimeUnit.MILLISECONDS);
        }
      } catch (InterruptedException e) {
        done.set(true);
        // We expect all iterators to either make progress together or finish.
        // This starts the cleanup process to halt all workers.
        worker.interrupt();
        throw new RuntimeException(e);
      }
      if (t == null) {
        done.set(true);
        return false;
      }
      return true;
    }

    private boolean mightHaveNext() {
      return !done.get() || !queue.isEmpty();
    }

    @Override
    public T next() {
      Preconditions.checkState(t != null, "next element cannot be null");
      if (rethrow != null) {
        throw rethrow;
      }
      return t;
    }
  }
}
