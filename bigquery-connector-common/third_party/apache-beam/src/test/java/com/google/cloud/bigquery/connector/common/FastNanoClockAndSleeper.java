package com.google.cloud.bigquery.connector.common;

import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/**
 * This object quickly moves time forward based upon how much it has been asked to sleep, without
 * actually sleeping, to simulate the backoff.
 */
public class FastNanoClockAndSleeper extends ExternalResource
    implements NanoClock, Sleeper, TestRule {
  private AtomicLong fastNanoTime = new AtomicLong();

  @Override
  public long nanoTime() {
    return fastNanoTime.get();
  }

  @Override
  protected void before() throws Throwable {
    fastNanoTime = new AtomicLong(NanoClock.SYSTEM.nanoTime());
  }

  @Override
  public void sleep(long millis) throws InterruptedException {
    fastNanoTime.addAndGet(millis * 1000000L);
  }
}
