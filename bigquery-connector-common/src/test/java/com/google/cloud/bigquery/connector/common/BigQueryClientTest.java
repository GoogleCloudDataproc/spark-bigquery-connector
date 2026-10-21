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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class BigQueryClientTest {

  private static final TableId TABLE_ID = TableId.of("project", "dataset", "table");
  private static final Schema SCHEMA = Schema.of(Field.of("name", StandardSQLTypeName.STRING));
  private static final long METADATA_ROW_COUNT = 42L;
  private static final long QUERY_ROW_COUNT = 7L;

  private BigQuery bigQuery;
  private BigQueryClient bigQueryClient;

  @Before
  public void setUp() throws Exception {
    bigQuery = mock(BigQuery.class);
    when(bigQuery.getOptions())
        .thenReturn(
            BigQueryOptions.newBuilder()
                .setProjectId("parent-project")
                .setCredentials(NoCredentials.getInstance())
                .build());
    TableResult countResult = mock(TableResult.class);
    when(countResult.iterateAll())
        .thenReturn(
            ImmutableList.of(
                FieldValueList.of(
                    ImmutableList.of(
                        FieldValue.of(
                            FieldValue.Attribute.PRIMITIVE, String.valueOf(QUERY_ROW_COUNT))))));
    when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(countResult);

    bigQueryClient =
        new BigQueryClient(
            bigQuery,
            /* materializationProject= */ Optional.empty(),
            /* materializationDataset= */ Optional.empty(),
            CacheBuilder.newBuilder().build(),
            ImmutableMap.of(),
            QueryJobConfiguration.Priority.INTERACTIVE,
            /* jobCompletionListener= */ Optional.empty(),
            /* bigQueryJobTimeoutInMinutes= */ 60);
  }

  @Test
  public void testCalculateTableSizeUsesMetadataWhenAllowed() throws Exception {
    long size = bigQueryClient.calculateTableSize(nativeTable(), Optional.empty(), true);

    assertThat(size).isEqualTo(METADATA_ROW_COUNT);
    verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
  }

  @Test
  public void testCalculateTableSizeRunsQueryByDefault() throws Exception {
    assertThat(bigQueryClient.calculateTableSize(nativeTable(), Optional.empty(), false))
        .isEqualTo(QUERY_ROW_COUNT);
    assertThat(bigQueryClient.calculateTableSize(nativeTable(), Optional.empty()))
        .isEqualTo(QUERY_ROW_COUNT);
  }

  @Test
  public void testCalculateTableSizeRunsQueryWithFilterEvenWhenMetadataAllowed() throws Exception {
    long size = bigQueryClient.calculateTableSize(nativeTable(), Optional.of("name = 'a'"), true);

    assertThat(size).isEqualTo(QUERY_ROW_COUNT);
    ArgumentCaptor<QueryJobConfiguration> queryCaptor =
        ArgumentCaptor.forClass(QueryJobConfiguration.class);
    verify(bigQuery).query(queryCaptor.capture());
    assertThat(queryCaptor.getValue().getQuery()).contains("WHERE name = 'a'");
  }

  @Test
  public void testCalculateTableSizeRunsQueryWhenMetadataRowCountMissing() throws Exception {
    TableInfo table =
        TableInfo.of(TABLE_ID, StandardTableDefinition.newBuilder().setSchema(SCHEMA).build());

    assertThat(bigQueryClient.calculateTableSize(table, Optional.empty(), true))
        .isEqualTo(QUERY_ROW_COUNT);
  }

  @Test
  public void testCalculateTableSizeRunsQueryForNonNativeTablesEvenWhenMetadataAllowed()
      throws Exception {
    TableInfo externalTable =
        TableInfo.of(
            TABLE_ID,
            ExternalTableDefinition.of("gs://bucket/data.csv", SCHEMA, FormatOptions.csv()));
    TableInfo view = TableInfo.of(TABLE_ID, ViewDefinition.of("SELECT 1 AS name"));

    assertThat(bigQueryClient.calculateTableSize(externalTable, Optional.empty(), true))
        .isEqualTo(QUERY_ROW_COUNT);
    assertThat(bigQueryClient.calculateTableSize(view, Optional.empty(), true))
        .isEqualTo(QUERY_ROW_COUNT);
  }

  private static TableInfo nativeTable() {
    return TableInfo.of(
        TABLE_ID,
        StandardTableDefinition.newBuilder()
            .setSchema(SCHEMA)
            .setNumRows(METADATA_ROW_COUNT)
            .build());
  }
}
