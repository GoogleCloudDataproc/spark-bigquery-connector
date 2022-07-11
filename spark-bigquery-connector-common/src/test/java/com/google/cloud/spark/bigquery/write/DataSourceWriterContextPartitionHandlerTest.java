/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.write;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spark.bigquery.write.context.DataWriterContext;
import com.google.cloud.spark.bigquery.write.context.DataWriterContextFactory;
import com.google.cloud.spark.bigquery.write.context.WriterCommitMessageContext;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceWriterContextPartitionHandlerTest {

  private static final long EPOCH = 1000;

  @Test
  public void testGoodWrite() throws Exception {
    WriterCommitMessageContext writerCommitMessage = new WriterCommitMessageContext() {};
    DataWriterContext dataWriterContext = mock(DataWriterContext.class);
    when(dataWriterContext.commit()).thenReturn(writerCommitMessage);
    DataWriterContextFactory dataWriterContextFactory = mock(DataWriterContextFactory.class);
    when(dataWriterContextFactory.createDataWriterContext(any(Integer.class), anyLong(), anyLong()))
        .thenReturn(dataWriterContext);
    DataSourceWriterContextPartitionHandler handler =
        new DataSourceWriterContextPartitionHandler(dataWriterContextFactory, EPOCH);

    Iterator<Row> rowIterator =
        Iterators.forArray(
            new GenericRow(new Object[] {1, "a"}), new GenericRow(new Object[] {2, "b"}));

    Iterator<WriterCommitMessageContext> result = handler.call(0, rowIterator);

    verify(dataWriterContext, atLeast(2)).write(any(InternalRow.class));
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isSameInstanceAs(writerCommitMessage);
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  public void testBadWrite() throws Exception {
    DataWriterContext dataWriterContext = mock(DataWriterContext.class);
    doThrow(new IOException("testing bad write"))
        .when(dataWriterContext)
        .write(any(InternalRow.class));
    DataWriterContextFactory dataWriterContextFactory = mock(DataWriterContextFactory.class);
    when(dataWriterContextFactory.createDataWriterContext(any(Integer.class), anyLong(), anyLong()))
        .thenReturn(dataWriterContext);
    DataSourceWriterContextPartitionHandler handler =
        new DataSourceWriterContextPartitionHandler(dataWriterContextFactory, EPOCH);

    Iterator<Row> rowIterator =
        Iterators.forArray(
            new GenericRow(new Object[] {1, "a"}), new GenericRow(new Object[] {2, "b"}));

    Iterator<WriterCommitMessageContext> resultIterator = handler.call(0, rowIterator);

    verify(dataWriterContext, atLeastOnce()).write(any(InternalRow.class));
    verify(dataWriterContext).abort();
    List<WriterCommitMessageContext> result =
        Streams.stream(resultIterator).collect(Collectors.toList());
    assertThat(resultIterator.hasNext()).isFalse();
  }
}
