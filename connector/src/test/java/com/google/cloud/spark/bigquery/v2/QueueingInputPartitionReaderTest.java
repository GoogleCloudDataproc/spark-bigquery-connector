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

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.truth.Correspondence;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

public class QueueingInputPartitionReaderTest {

  @Test
  public void testReadsAllBatchesInRoundRobin() throws Exception {

    ColumnarBatch[] batches = new ColumnarBatch[6];
    for (int x = 0; x < batches.length; x++) {
      batches[x] = new ColumnarBatch(new ColumnVector[0]);
    }
    InputPartitionReader<ColumnarBatch> r1 = mock(InputPartitionReader.class);
    when(r1.next()).thenReturn(true).thenReturn(false);
    when(r1.get()).thenReturn(batches[0]);
    InputPartitionReader<ColumnarBatch> r2 = mock(InputPartitionReader.class);
    when(r2.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(r2.get()).thenReturn(batches[1]).thenReturn(batches[3]);
    InputPartitionReader<ColumnarBatch> r3 = mock(InputPartitionReader.class);
    when(r3.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
    when(r3.get()).thenReturn(batches[2]).thenReturn(batches[4]).thenReturn(batches[5]);

    ExecutorService executor = Executors.newFixedThreadPool(3);
    QueueingInputPartitionReader reader =
        new QueueingInputPartitionReader(ImmutableList.of(r1, r2, r3), executor);
    List<ColumnarBatch> read = new ArrayList<>();
    while (reader.next()) {
      read.add(reader.get());
    }
    reader.close();

    assertThat(read).comparingElementsUsing(Correspondence.from((a, b) -> {
      return a == b;
    }, "same")).containsExactlyElementsIn(batches).inOrder();
    assertThat(executor.isShutdown()).isTrue();
  }

  @Test
  public void testExceptionIsPropagatedFromNext() throws Exception {

    IOException exception = new IOException("an exception");
    InputPartitionReader<ColumnarBatch> r1 = mock(InputPartitionReader.class);
    when(r1.next()).thenThrow(exception);

    ExecutorService executor = MoreExecutors.newDirectExecutorService();
    QueueingInputPartitionReader reader =
        new QueueingInputPartitionReader(ImmutableList.of(r1), executor);

    IOException e = Assert.assertThrows(IOException.class, reader::next);

    assertThat(e).isSameInstanceAs(exception);
  }

  @Test
  public void testInterruptsOnClose() throws Exception {
    InputPartitionReader<ColumnarBatch> r1 = mock(InputPartitionReader.class);
    when(r1.next()).thenReturn(true);
    when(r1.get()).thenReturn(new ColumnarBatch(new ColumnVector[0]));
    CountDownLatch latch = new CountDownLatch(1);
    InputPartitionReader<ColumnarBatch> r2 = mock(InputPartitionReader.class);
    when(r2.next())
        .thenAnswer(
            (InvocationOnMock invocation) -> {
              latch.countDown();
              MILLISECONDS.sleep(QueueingInputPartitionReader.POLL_TIME);
              return true;
            });
    when(r2.get())
        .thenAnswer(
            (InvocationOnMock invocation) -> {
              MILLISECONDS.sleep(QueueingInputPartitionReader.POLL_TIME);
              return new ColumnarBatch(new ColumnVector[0]);
            });

    ExecutorService executor = Executors.newSingleThreadExecutor();
    QueueingInputPartitionReader reader =
        new QueueingInputPartitionReader(ImmutableList.of(r1, r2), executor);

    ExecutorService oneOff = Executors.newSingleThreadExecutor();
    Instant start = Instant.now();

    Future<Instant> endTime =
        oneOff.submit(
            () -> {
              try {
                while (reader.next()) {
                  reader.get();
                }
              } catch (IOException e) {
                e.printStackTrace();
                return Instant.ofEpochMilli(0);
              }
              return Instant.now();
            });

    // Wait until next gets called.
    latch.await();
    // Should interrupt blocking operations.
    reader.close();
    oneOff.shutdown();

    assertThat(endTime.get()).isGreaterThan(start);
    assertThat(Duration.between(start, endTime.get()))
        .isLessThan(Duration.ofMillis(QueueingInputPartitionReader.POLL_TIME * 2));


  }
}
