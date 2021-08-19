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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class IntegrationTestUtils {

  static Logger logger = LoggerFactory.getLogger(IntegrationTestUtils.class);

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
    BigQueryClient bigQueryClient = new BigQueryClient(getBigquery(), Optional.empty(),
        Optional.empty());
    logger.warn("Running query '{}'", query);
    bigQueryClient.query(query);
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
    return SparkSession.builder()
        .appName(applicationName)
        .master("local")
        .getOrCreate();
  }

  static void createView(String dataset, String table, String view) {
    BigQuery bq = getBigquery();
    String query = String.format("SELECT * FROM %s.%s", dataset, table);
    TableId tableId = TableId.of(dataset, view);
    ViewDefinition viewDefinition =
        ViewDefinition.newBuilder(query).setUseLegacySql(false).build();
    bq.create(TableInfo.of(tableId, viewDefinition));
  }

  public static IntegrationTestContext initialize(
      Class<? extends SparkBigQueryIntegrationTestBase> testClass, boolean createAllTypesTable) {
    SparkSession spark = getOrCreateSparkSession(testClass.getSimpleName());
    String testDataset = String
        .format("spark_bigquery_%s_%d", testClass.getSimpleName(), System.currentTimeMillis());
    createDataset(testDataset);
    if (createAllTypesTable) {
      runQuery(String.format(
          TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE,
          testDataset, TestConstants.ALL_TYPES_TABLE_NAME));
      createView(testDataset, TestConstants.ALL_TYPES_TABLE_NAME,
          TestConstants.ALL_TYPES_VIEW_NAME);
      runQuery(String.format(
          TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_QUERY_TEMPLATE,
          testDataset, TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_NAME));
    }
    return new IntegrationTestContext(spark, testDataset);
  }

  public static void clean(IntegrationTestContext ctx) {
    deleteDatasetAndTables(ctx.getTestDataset());
  }
}
