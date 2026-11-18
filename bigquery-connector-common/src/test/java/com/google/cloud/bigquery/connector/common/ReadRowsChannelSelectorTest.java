/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ReadRowsChannelSelectorTest {

  @Test
  public void rejectsNonPositiveNumberOfChannels() {
    assertThrows(IllegalArgumentException.class, () -> new ReadRowsChannelSelector(0));
  }

  @Test
  public void spreadsStreamsOverIdleChannels() {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(3);

    assertThat(selector.acquire()).isEqualTo(0);
    assertThat(selector.acquire()).isEqualTo(1);
    assertThat(selector.acquire()).isEqualTo(2);
    assertThat(selector.getActiveStreams()).asList().containsExactly(1, 1, 1).inOrder();
  }

  @Test
  public void picksTheLeastLoadedChannel() {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(3);
    int longRunning = selector.acquire();

    // Short streams start and end while the first one is still active: none of them shares its
    // channel, unlike with round-robin, where every third stream would.
    for (int i = 0; i < 10; i++) {
      int channel = selector.acquire();
      assertThat(channel).isNotEqualTo(longRunning);
      selector.release(channel);
    }
    assertThat(selector.getActiveStreams()[longRunning]).isEqualTo(1);
  }

  @Test
  public void rotatesBetweenEquallyLoadedChannels() {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(3);
    List<Integer> channels = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      int channel = selector.acquire();
      channels.add(channel);
      selector.release(channel);
    }

    assertThat(channels).containsExactly(0, 1, 2, 0, 1, 2).inOrder();
  }

  @Test
  public void sharesChannelsEvenlyWhenThereAreMoreStreamsThanChannels() {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(3);
    for (int i = 0; i < 7; i++) {
      selector.acquire();
    }

    assertThat(selector.getActiveStreams()).asList().containsExactly(3, 2, 2).inOrder();
  }

  @Test
  public void releaseOfAnIdleChannelKeepsTheCountAtZero() {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(2);
    selector.release(1);

    assertThat(selector.getActiveStreams()).asList().containsExactly(0, 0).inOrder();
  }

  @Test
  public void rejectsReleaseOfAnUnknownChannel() {
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(2);

    assertThrows(IndexOutOfBoundsException.class, () -> selector.release(2));
    assertThrows(IndexOutOfBoundsException.class, () -> selector.release(-1));
  }

  @Test
  public void keepsCountsConsistentUnderConcurrentUse() throws Exception {
    int numChannels = 4;
    int numThreads = 8;
    int iterationsPerThread = 10_000;
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(numChannels);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < numThreads; t++) {
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < iterationsPerThread; i++) {
                    int channel = selector.acquire();
                    assertThat(channel).isAtLeast(0);
                    assertThat(channel).isLessThan(numChannels);
                    selector.release(channel);
                  }
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }

    assertThat(selector.getActiveStreams()).asList().containsExactly(0, 0, 0, 0);
  }

  @Test
  public void concurrentStreamsGetSeparateChannelsWhenThePoolIsLargeEnough() throws Exception {
    int numChannels = 8;
    ReadRowsChannelSelector selector = new ReadRowsChannelSelector(numChannels);
    ExecutorService executor = Executors.newFixedThreadPool(numChannels);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<Integer>> futures = new ArrayList<>();
      for (int t = 0; t < numChannels; t++) {
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  return selector.acquire();
                }));
      }
      start.countDown();
      List<Integer> channels = new ArrayList<>();
      for (Future<Integer> future : futures) {
        channels.add(future.get(30, TimeUnit.SECONDS));
      }

      assertThat(channels).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
    } finally {
      executor.shutdownNow();
    }
  }
}
