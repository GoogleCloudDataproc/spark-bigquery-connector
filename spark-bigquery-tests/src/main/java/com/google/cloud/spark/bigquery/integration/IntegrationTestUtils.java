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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import java.util.Optional;
import org.apache.spark.bigquery.BigNumeric;
import org.apache.spark.bigquery.BigNumericUDT;
import org.apache.spark.bigquery.BigQueryDataTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      Class<? extends SparkBigQueryIntegrationTestBase> testClass) {
    SparkSession spark = getOrCreateSparkSession(testClass.getSimpleName());
    String testDataset = String
        .format("spark_bigquery_%s_%d", testClass.getSimpleName(), System.currentTimeMillis());
    createDataset(testDataset);
    runQuery(String.format(
        TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE,
        testDataset, TestConstants.ALL_TYPES_TABLE_NAME));
    createView(testDataset, TestConstants.ALL_TYPES_TABLE_NAME,
        TestConstants.ALL_TYPES_VIEW_NAME);
    runQuery(String.format(
        TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_QUERY_TEMPLATE,
        testDataset, TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_NAME));
    return new IntegrationTestContext(spark, testDataset);
  }

  public static void clean(IntegrationTestContext ctx) {
    deleteDatasetAndTables(ctx.getTestDataset());
  }

  public static void compareBigNumericDataSetRows(Row row, Row expected) {
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
      assertThat(value).isEqualTo(expected.get(i));
      //     }
    }
  }

  private static String getBigNumericString(Row row, int i, int j) {
    BigNumeric bigNumericValue = (BigNumeric) (((GenericRowWithSchema) row.get(i)).get(j));
    String bigNumericString = bigNumericValue.getNumber().toPlainString();
    return bigNumericString;
  }

  public static void compareBigNumericDataSetSchema(StructType actualSchema,
      StructType expectedSchema) {

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
