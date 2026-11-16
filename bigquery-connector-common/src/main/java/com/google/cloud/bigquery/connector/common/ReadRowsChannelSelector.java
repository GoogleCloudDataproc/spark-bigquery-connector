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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assigns ReadRows streams to the channels of a BigQueryReadClient's channel pool, picking the
 * channel with the fewest active streams.
 *
 * <p>Without a channel affinity, the gax channel pool assigns calls round-robin, regardless of how
 * many streams each channel is already serving. Since a ReadRows stream can stay open for a long
 * time, round-robin can place several concurrent streams on the same connection while other
 * connections are idle, and all of them then share that connection's throughput. Picking the least
 * loaded channel keeps concurrent streams on separate connections whenever the pool has enough
 * channels. Ties are broken by rotating the starting channel, so that equally loaded channels are
 * used evenly.
 *
 * <p>This is a better default than round-robin for long-lived streams regardless of the transport
 * or of what makes a connection slow (network path, proxy, or server), since it limits the number
 * of streams that a single slow connection can affect.
 *
 * <p>The returned channel index is meant to be passed as the channel affinity of the ReadRows call,
 * so the selector must be created with the same number of channels as the client's pool. If gax's
 * channel pool ever provides least-loaded selection itself, prefer it and remove this class.
 */
public class ReadRowsChannelSelector {
  private static final Logger log = LoggerFactory.getLogger(ReadRowsChannelSelector.class);

  private final int[] activeStreams;
  private int nextStart = 0;

  /** Creates a selector for a channel pool with {@code numChannels} channels. */
  public ReadRowsChannelSelector(int numChannels) {
    Preconditions.checkArgument(
        numChannels > 0, "numChannels must be positive. Received: %s", numChannels);
    this.activeStreams = new int[numChannels];
  }

  /**
   * Returns the channel with the fewest active streams, and counts a new active stream on it. Each
   * call must be paired with a call to {@link #release(int)} once the stream ends.
   */
  public synchronized int acquire() {
    int numChannels = activeStreams.length;
    int selected = nextStart;
    for (int i = 1; i < numChannels; i++) {
      int candidate = (nextStart + i) % numChannels;
      if (activeStreams[candidate] < activeStreams[selected]) {
        selected = candidate;
      }
    }
    activeStreams[selected]++;
    nextStart = (selected + 1) % numChannels;
    return selected;
  }

  /** Counts one fewer active stream on the given channel, previously returned by acquire(). */
  public synchronized void release(int channel) {
    Preconditions.checkElementIndex(channel, activeStreams.length, "channel");
    if (activeStreams[channel] > 0) {
      activeStreams[channel]--;
    } else {
      log.warn("Released channel {} which has no active ReadRows streams", channel);
    }
  }

  public int getNumChannels() {
    return activeStreams.length;
  }

  @VisibleForTesting
  synchronized int[] getActiveStreams() {
    return activeStreams.clone();
  }
}
