/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BigQueryEstimatedTableStatisticsTest {

  public static final TableId TABLE_ID = TableId.of("dataset", "table");
  public static final StandardTableDefinition TABLE_DEFINITION =
      StandardTableDefinition.newBuilder().setNumBytes(1000L).setNumRows(10L).build();
  public static final TableInfo TABLE = mock(TableInfo.class);

  @BeforeClass
  public static void prepareMocks() {
    when(TABLE.getNumBytes()).thenReturn(TABLE_DEFINITION.getNumBytes());
    when(TABLE.getNumRows()).thenReturn(BigInteger.valueOf(TABLE_DEFINITION.getNumRows()));
    when(TABLE.getDefinition()).thenReturn(TABLE_DEFINITION);
  }

  @Test
  public void testNotATable() {
    TableInfo table =
        TableInfo.of(TABLE_ID, ViewDefinition.newBuilder("select * from foo.bar").build());
    BigQueryEstimatedTableStatistics s =
        BigQueryEstimatedTableStatistics.newBuilder(table, null).build();
    assertThat(s.numRows().isPresent()).isFalse();
    assertThat(s.sizeInBytes().isPresent()).isFalse();
  }

  @Test
  public void testNoFilterNoProjection() {
    BigQueryEstimatedTableStatistics s =
        BigQueryEstimatedTableStatistics.newBuilder(TABLE, null).build();
    assertThat(s.numRows().isPresent()).isTrue();
    assertThat(s.numRows().getAsLong()).isEqualTo(10L);
    assertThat(s.sizeInBytes().isPresent()).isTrue();
    assertThat(s.sizeInBytes().getAsLong()).isEqualTo(1000L);
  }

  @Test
  public void testWithFilterNoProjection() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    prepareCalculateSize(bigQueryClient);
    BigQueryEstimatedTableStatistics s =
        BigQueryEstimatedTableStatistics.newBuilder(TABLE, bigQueryClient)
            .setFilter(Optional.of("some_filter = 1"))
            .build();
    assertThat(s.numRows().isPresent()).isTrue();
    assertThat(s.numRows().getAsLong()).isEqualTo(3L);
    assertThat(s.sizeInBytes().isPresent()).isTrue();
    assertThat(s.sizeInBytes().getAsLong()).isEqualTo(300L);
  }

  @Test
  public void testNoFilterWithProjection() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    prepareDryRun(bigQueryClient);
    BigQueryEstimatedTableStatistics s =
        BigQueryEstimatedTableStatistics.newBuilder(TABLE, bigQueryClient)
            .setSelectedColumns(Optional.of(ImmutableList.of("col_a", "col_b")))
            .build();
    assertThat(s.numRows().isPresent()).isTrue();
    assertThat(s.numRows().getAsLong()).isEqualTo(10L);
    assertThat(s.sizeInBytes().isPresent()).isTrue();
    assertThat(s.sizeInBytes().getAsLong()).isEqualTo(400L);
  }

  @Test
  public void testWithFilterWithProjection() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    prepareCalculateSize(bigQueryClient);
    prepareDryRun(bigQueryClient);
    BigQueryEstimatedTableStatistics s =
        BigQueryEstimatedTableStatistics.newBuilder(TABLE, bigQueryClient)
            .setFilter(Optional.of("some_filter = 1"))
            .setSelectedColumns(Optional.of(ImmutableList.of("col_a", "col_b")))
            .build();
    assertThat(s.numRows().isPresent()).isTrue();
    assertThat(s.numRows().getAsLong()).isEqualTo(3L);
    assertThat(s.sizeInBytes().isPresent()).isTrue();
    assertThat(s.sizeInBytes().getAsLong()).isEqualTo(120L);
  }

  private void prepareDryRun(BigQueryClient mockBigQueryClient) {
    JobStatistics.QueryStatistics mockQueryStatistics = mock(JobStatistics.QueryStatistics.class);
    when(mockQueryStatistics.getTotalBytesProcessed()).thenReturn(400L);
    Job mockJob = mock(Job.class);
    when(mockJob.getStatistics()).thenReturn(mockQueryStatistics);
    when(mockBigQueryClient.dryRunQuery(any())).thenReturn(mockJob);
    when(mockBigQueryClient.createSql(any(TableId.class), any(ImmutableList.class), any()))
        .thenReturn("");
  }

  private void prepareCalculateSize(BigQueryClient mockBigQueryClient) {
    when(mockBigQueryClient.calculateTableSize(any(TableInfo.class), any(Optional.class)))
        .thenReturn(3L);
  }
}
