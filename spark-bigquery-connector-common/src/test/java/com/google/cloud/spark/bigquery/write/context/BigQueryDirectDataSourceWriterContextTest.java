/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.spark.bigquery.write.context;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.spark.bigquery.PartitionOverwriteMode;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import java.time.ZoneId;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryDirectDataSourceWriterContextTest {

  @Mock BigQueryClient bigQueryClient;

  @Mock BigQueryClientFactory bigQueryClientFactory;

  @Mock RetrySettings bigqueryDataWriterHelperRetrySettings;

  TableId destinationTableId = TableId.of("dataset", "table");
  String writeUUID = "00000000-0000-0000-0000-000000000000";
  StructType sparkSchema =
      new StructType(
          new StructField[] {
            StructField.apply("foo", DataTypes.StringType, true, Metadata.empty())
          });

  TableInfo destinationTable =
      TableInfo.newBuilder(
              destinationTableId,
              StandardTableDefinition.newBuilder()
                  .setSchema(
                      Schema.of(
                          Field.newBuilder("foo", LegacySQLTypeName.STRING)
                              .setMode(Field.Mode.NULLABLE)
                              .build()))
                  .build())
          .build();
  TableInfo tempTable =
      TableInfo.newBuilder(
              TableId.of("dataset", "temp_table"),
              StandardTableDefinition.newBuilder()
                  .setSchema(
                      Schema.of(
                          Field.newBuilder("foo", LegacySQLTypeName.STRING)
                              .setMode(Field.Mode.NULLABLE)
                              .build()))
                  .build())
          .build();

  @Test
  public void testDeleteOnAbort_saveModeAppend() {
    when(bigQueryClient.tableExists(any())).thenReturn(true);
    when(bigQueryClient.getTable(any())).thenReturn(destinationTable);
    when(bigQueryClient.createTablePathForBigQueryStorage(any())).thenReturn("");
    BigQueryDirectDataSourceWriterContext ctx =
        createBigQueryDirectDataSourceWriterContext(SaveMode.Append);
    ctx.abort(null);
    verify(bigQueryClient, times(0)).deleteTable(any());
  }

  @Test
  public void testDeleteOnAbort_saveModeErrorIfExists() {
    when(bigQueryClient.tableExists(any())).thenReturn(true);
    when(bigQueryClient.getTable(any())).thenReturn(destinationTable);
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          BigQueryDirectDataSourceWriterContext ctx =
              createBigQueryDirectDataSourceWriterContext(SaveMode.ErrorIfExists);
        });
  }

  @Test
  public void testDeleteOnAbort_saveModeIgnore() {
    when(bigQueryClient.tableExists(any())).thenReturn(true);
    when(bigQueryClient.getTable(any())).thenReturn(destinationTable);
    when(bigQueryClient.createTablePathForBigQueryStorage(any())).thenReturn("");
    BigQueryDirectDataSourceWriterContext ctx =
        createBigQueryDirectDataSourceWriterContext(SaveMode.Ignore);
    ctx.abort(null);
    verify(bigQueryClient, times(0)).deleteTable(any());
  }

  @Test
  public void testDeleteOnAbort_saveModeOverwrite() {
    when(bigQueryClient.tableExists(any())).thenReturn(true);
    when(bigQueryClient.getTable(any())).thenReturn(destinationTable);
    when(bigQueryClient.createTempTable(any(), any(), any())).thenReturn(tempTable);
    when(bigQueryClient.createTablePathForBigQueryStorage(any())).thenReturn("");
    BigQueryDirectDataSourceWriterContext ctx =
        createBigQueryDirectDataSourceWriterContext(SaveMode.Overwrite);
    ctx.abort(null);
    verify(bigQueryClient, times(1)).deleteTable(any());
  }

  @Test
  public void testDeleteOnAbort_newTable() {
    when(bigQueryClient.tableExists(destinationTableId)).thenReturn(false);
    when(bigQueryClient.createTable(any(), any(), any())).thenReturn(destinationTable);
    when(bigQueryClient.createTablePathForBigQueryStorage(any())).thenReturn("");
    BigQueryDirectDataSourceWriterContext ctx =
        createBigQueryDirectDataSourceWriterContext(SaveMode.Append);
    ctx.abort(null);
    verify(bigQueryClient, times(1)).deleteTable(any());
  }

  private BigQueryDirectDataSourceWriterContext createBigQueryDirectDataSourceWriterContext(
      SaveMode saveMode) {
    return new BigQueryDirectDataSourceWriterContext(
        bigQueryClient,
        bigQueryClientFactory,
        destinationTableId,
        writeUUID,
        saveMode,
        sparkSchema,
        bigqueryDataWriterHelperRetrySettings,
        Optional.absent(),
        true,
        ImmutableMap.<String, String>builder().build(),
        SchemaConvertersConfiguration.of(ZoneId.of("UTC")),
        java.util.Optional.empty(),
        false,
        PartitionOverwriteMode.STATIC);
  }
}
