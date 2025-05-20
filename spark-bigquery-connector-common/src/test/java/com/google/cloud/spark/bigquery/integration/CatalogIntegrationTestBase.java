/*
 * Copyright 2025 Google Inc. All Rights Reserved.
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
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class CatalogIntegrationTestBase {

  public static final String DEFAULT_NAMESPACE = "default";
  @ClassRule public static TestDataset testDataset = new TestDataset();

  BigQuery bigquery = IntegrationTestUtils.getBigquery();

  private String testTable;

  @Before
  public void renameTestTable() {
    testTable =
        String.format(
            "test_%s_%s",
            Long.toHexString(System.currentTimeMillis()), Long.toHexString(System.nanoTime()));
  }

  @After
  public void cleanTestTable() throws Exception {
    Table table = bigquery.getTable(TableId.of("default", testTable));
    if (table != null) {
      table.delete();
    }
  }

  @Test
  public void testCreateTableInDefaultNamespace() throws Exception {
    internalTestCreateTable(DEFAULT_NAMESPACE);
  }

  @Test
  public void testCreateTableInCustomNamespace() throws Exception {
    internalTestCreateTable(testDataset.testDataset);
  }

  private void internalTestCreateTable(String dataset) throws InterruptedException {
    assertThat(bigquery.getDataset(DatasetId.of(dataset))).isNotNull();
    try (SparkSession spark = createSparkSession()) {
      spark.sql("CREATE TABLE " + fullTableName(dataset) + "(id int, data string);");
      Table table = bigquery.getTable(TableId.of(dataset, testTable));
      assertThat(table).isNotNull();
      assertThat(selectCountStarFrom(dataset, testTable)).isEqualTo(0L);
    }
  }

  @Test
  public void testCreateTableAndInsertInDefaultNamespace() throws Exception {
    internalTestCreateTableAndInsert(DEFAULT_NAMESPACE);
  }

  @Test
  public void testCreateTableAndInsertInCustomNamespace() throws Exception {
    internalTestCreateTableAndInsert(testDataset.testDataset);
  }

  private void internalTestCreateTableAndInsert(String dataset) throws InterruptedException {
    assertThat(bigquery.getDataset(DatasetId.of(dataset))).isNotNull();
    try (SparkSession spark = createSparkSession()) {
      spark.sql("CREATE TABLE " + fullTableName(dataset) + "(id int, data string);");
      spark.sql(String.format("INSERT INTO `%s`.`%s` VALUES (1, 'foo');", dataset, testTable));
      Table table = bigquery.getTable(TableId.of(dataset, testTable));
      assertThat(table).isNotNull();
      assertThat(selectCountStarFrom(dataset, testTable)).isEqualTo(1L);
    }
  }

  @Test
  public void testCreateTableAsSelectInDefaultNamespace() throws Exception {
    internalTestCreateTableAsSelect(DEFAULT_NAMESPACE);
  }

  @Test
  public void testCreateTableAsSelectInCustomNamespace() throws Exception {
    internalTestCreateTableAsSelect(testDataset.testDataset);
  }

  private void internalTestCreateTableAsSelect(String dataset) throws InterruptedException {
    assertThat(bigquery.getDataset(DatasetId.of(dataset))).isNotNull();
    try (SparkSession spark = createSparkSession()) {
      spark.sql("CREATE TABLE " + fullTableName(dataset) + " AS SELECT 1 AS id, 'foo' AS data;");
      Table table = bigquery.getTable(TableId.of(dataset, testTable));
      assertThat(table).isNotNull();
      assertThat(selectCountStarFrom(dataset, testTable)).isEqualTo(1L);
    }
  }

  @Test
  @Ignore("unsupported")
  public void testCreateTableWithExplicitTargetInDefaultNamespace() throws Exception {
    internalTestCreateTableWithExplicitTarget(DEFAULT_NAMESPACE);
  }

  @Test
  @Ignore("unsupported")
  public void testCreateTableWithExplicitTargetInCustomNamespace() throws Exception {
    internalTestCreateTableWithExplicitTarget(testDataset.testDataset);
  }

  private void internalTestCreateTableWithExplicitTarget(String dataset)
      throws InterruptedException {
    assertThat(bigquery.getDataset(DatasetId.of(dataset))).isNotNull();
    try (SparkSession spark = createSparkSession()) {
      spark.sql(
          "CREATE TABLE "
              + fullTableName(dataset)
              + " OPTIONS (table='bigquery-public-data.samples.shakespeare')");
      List<Row> result =
          spark
              .sql(
                  "SELECT word, SUM(word_count) FROM "
                      + fullTableName(dataset)
                      + " WHERE word='spark' GROUP BY word;")
              .collectAsList();
      assertThat(result).hasSize(1);
      Row resultRow = result.get(0);
      assertThat(resultRow.getString(0)).isEqualTo("spark");
      assertThat(resultRow.getLong(1)).isEqualTo(10L);
    }
  }

  private String fullTableName(String dataset) {
    return dataset.equals(DEFAULT_NAMESPACE)
        ? "`" + testTable + "`"
        : "`" + dataset + "`.`" + testTable + "`";
  }

  // this is needed as with direct write the table's metadata can e updated only after few minutes.
  // Queries take pending data into account though.
  private long selectCountStarFrom(String dataset, String table) throws InterruptedException {
    return bigquery
        .query(
            QueryJobConfiguration.of(
                String.format("SELECT COUNT(*) FROM `%s`.`%s`", dataset, table)))
        .getValues()
        .iterator()
        .next()
        .get(0)
        .getLongValue();
  }

  @Test
  public void testReadFromDifferentBigQueryProject() throws Exception {
    try (SparkSession spark = createSparkSession()) {
      List<Row> df =
          spark
              .sql(
                  "SELECT * from `bigquery-public-data`.`samples`.`shakespeare` WHERE word='spark'")
              .collectAsList();
      assertThat(df).hasSize(9);
    }
  }

  private static SparkSession createSparkSession() {
    return SparkSession.builder()
        .appName("catalog test")
        .master("local[*]")
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
        .config("spark.sql.sources.default", "bigquery")
        .config("spark.datasource.bigquery.writeMethod", "direct")
        .config("spark.sql.defaultCatalog", "bigquery")
        .config("spark.sql.catalog.bigquery", "com.google.cloud.spark.bigquery.BigQueryCatalog")
        .getOrCreate();
  }
}
