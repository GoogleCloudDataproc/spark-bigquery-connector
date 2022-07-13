/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTestUtils {

  static Logger logger = LoggerFactory.getLogger(IntegrationTestUtils.class);

  private static Cache<String, TableInfo> destinationTableCache =
      CacheBuilder.newBuilder().expireAfterWrite(15, TimeUnit.MINUTES).maximumSize(1000).build();

  public static BigQuery getBigquery() {
    return BigQueryOptions.getDefaultInstance().getService();
  }

  public static void createDataset(String dataset) {
    BigQuery bq = getBigquery();
    DatasetId datasetId = DatasetId.of(dataset);
    logger.warn("Creating test dataset: {}", datasetId);
    bq.create(DatasetInfo.of(datasetId));
  }

  public static void runQuery(String query) {
    BigQueryClient bigQueryClient =
        new BigQueryClient(
            getBigquery(),
            Optional.empty(),
            Optional.empty(),
            destinationTableCache,
            ImmutableMap.of());
    bigQueryClient.query(query);
  }

  public static void createBigLakeTable(
      String dataset,
      String table,
      StructType schema,
      String sourceURI,
      FormatOptions formatOptions) {
    BigQuery bq = getBigquery();
    TableId tableId = TableId.of(dataset, table);
    TableInfo tableInfo =
        TableInfo.newBuilder(
                tableId,
                ExternalTableDefinition.newBuilder(
                        sourceURI, SchemaConverters.toBigQuerySchema(schema), formatOptions)
                    .setConnectionId(TestConstants.BIGLAKE_CONNECTION_ID)
                    .build())
            .build();
    bq.create(tableInfo);
  }

  public static void deleteDatasetAndTables(String dataset) {
    BigQuery bq = getBigquery();
    logger.warn("Deleting test dataset '{}' and its contents", dataset);
    bq.delete(DatasetId.of(dataset), BigQuery.DatasetDeleteOption.deleteContents());
  }

  static Metadata metadata(String key, String value) {
    MetadataBuilder metadata = new MetadataBuilder();
    metadata.putString(key, value);
    return metadata.build();
  }

  static SparkSession getOrCreateSparkSession(String applicationName) {
    return SparkSession.builder().appName(applicationName).master("local").getOrCreate();
  }

  static void createView(String dataset, String table, String view) {
    BigQuery bq = getBigquery();
    String query = String.format("SELECT * FROM %s.%s", dataset, table);
    TableId tableId = TableId.of(dataset, view);
    ViewDefinition viewDefinition = ViewDefinition.newBuilder(query).setUseLegacySql(false).build();
    bq.create(TableInfo.of(tableId, viewDefinition));
  }

  public static void compareRows(Row row, Row expected) {
    for (int i = 0; i < expected.length(); i++) {
      // if (i == TestConstants.BIG_NUMERIC_COLUMN_POSITION) {
      // TODO: Restore this code after
      //  https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/446
      //  is fixed
      //
      // for (int j = 0; j < 2; j++) {
      //   String bigNumericString = getBigNumericString(row, i, j);
      //   String expectedBigNumericString = getBigNumericString(expected, i, j);
      //   assertThat(bigNumericString).isEqualTo(expectedBigNumericString);
      // }
      // } else {
      Object value = row.get(i);
      assertWithMessage("value of field " + expected.schema().fields()[i])
          .that(value)
          .isEqualTo(expected.get(i));
      //     }
    }
  }

  public static void compareBigNumericDataSetSchema(
      StructType actualSchema, StructType expectedSchema) {

    StructField[] actualFields = actualSchema.fields();
    StructField[] expectedFields = expectedSchema.fields();

    for (int i = 0; i < actualFields.length; i++) {
      StructField actualField = actualFields[i];
      StructField expectedField = expectedFields[i];
      assertThat(actualField).isEqualTo(expectedField);
      // TODO: Restore this code after
      //  https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/446
      //  is fixed
      //
      // if (i == TestConstants.BIG_NUMERIC_COLUMN_POSITION) {
      //   for (int j = 0; j < 2; j++) {
      //     DataType actualFieldDataType = ((StructType) actualField.dataType()).fields()[j]
      //         .dataType();
      //     DataType expectedFieldDataType = ((StructType) expectedField.dataType()).fields()[j]
      //         .dataType();
      //     assertThat(actualFieldDataType).isEqualTo(DataTypes.StringType);
      //     assertThat(expectedFieldDataType).isEqualTo(BigQueryDataTypes.BigNumericType);
      //   }
      //
      // } else {
      //   assertThat(actualField).isEqualTo(expectedField);
      // }
    }
  }
}
