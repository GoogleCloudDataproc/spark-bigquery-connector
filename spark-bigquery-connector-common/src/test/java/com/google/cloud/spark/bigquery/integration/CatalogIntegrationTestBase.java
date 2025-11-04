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
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class CatalogIntegrationTestBase {

  public static final String DEFAULT_NAMESPACE = "default";
  @ClassRule public static TestDataset testDataset = new TestDataset();

  BigQuery bigquery = IntegrationTestUtils.getBigquery();

  protected static SparkSession spark;
  private String testTable;
  // 2. Initialize the SparkSession ONCE before all tests
  @BeforeClass
  public static void setupSparkSession() {
    spark =
        SparkSession.builder()
            .appName("catalog test")
            .master("local[*]")
            .config("spark.sql.legacy.createHiveTableByDefault", "false")
            .config("spark.sql.sources.default", "bigquery")
            .config("spark.datasource.bigquery.writeMethod", "direct")
            .config("spark.sql.defaultCatalog", "bigquery")
            .config("spark.sql.catalog.bigquery", "com.google.cloud.spark.bigquery.BigQueryCatalog")
            .getOrCreate();
  }

  // 4. Stop the SparkSession ONCE after all tests are done
  // This fixes the local IllegalStateException (race condition)
  @AfterClass
  public static void teardownSparkSession() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

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
    spark.sql("CREATE TABLE " + fullTableName(dataset) + "(id int, data string);");
    Table table = bigquery.getTable(TableId.of(dataset, testTable));
    assertThat(table).isNotNull();
    assertThat(selectCountStarFrom(dataset, testTable)).isEqualTo(0L);
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
    spark.sql("CREATE TABLE " + fullTableName(dataset) + "(id int, data string);");
    spark.sql(String.format("INSERT INTO `%s`.`%s` VALUES (1, 'foo');", dataset, testTable));
    Table table = bigquery.getTable(TableId.of(dataset, testTable));
    assertThat(table).isNotNull();
    assertThat(selectCountStarFrom(dataset, testTable)).isEqualTo(1L);
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
    spark.sql("CREATE TABLE " + fullTableName(dataset) + " AS SELECT 1 AS id, 'foo' AS data;");
    Table table = bigquery.getTable(TableId.of(dataset, testTable));
    assertThat(table).isNotNull();
    assertThat(selectCountStarFrom(dataset, testTable)).isEqualTo(1L);
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
    List<Row> df =
        spark
            .sql("SELECT * from `bigquery-public-data`.`samples`.`shakespeare` WHERE word='spark'")
            .collectAsList();
    assertThat(df).hasSize(9);
  }

  @Test
  public void testListNamespaces() throws Exception {
    String database =
        String.format("show_databases_test_%s_%s", System.currentTimeMillis(), System.nanoTime());
    DatasetId datasetId = DatasetId.of(database);
    bigquery.create(Dataset.newBuilder(datasetId).build());
    List<Row> databases = spark.sql("SHOW DATABASES").collectAsList();
    assertThat(databases).contains(RowFactory.create(database));
    bigquery.delete(datasetId);
  }

  @Test
  public void testCreateNamespace() throws Exception {
    String database =
        String.format("create_database_test_%s_%s", System.currentTimeMillis(), System.nanoTime());
    DatasetId datasetId = DatasetId.of(database);
    spark.sql("CREATE DATABASE " + database + ";");
    Dataset dataset = bigquery.getDataset(datasetId);
    assertThat(dataset).isNotNull();
    bigquery.delete(datasetId);
  }

  @Test
  public void testCreateNamespaceWithLocation() throws Exception {
    String database =
        String.format("create_database_test_%s_%s", System.currentTimeMillis(), System.nanoTime());
    DatasetId datasetId = DatasetId.of(database);
    spark.sql(
        "CREATE DATABASE "
            + database
            + " COMMENT 'foo' WITH DBPROPERTIES (bigquery_location = 'us-east1');");
    Dataset dataset = bigquery.getDataset(datasetId);
    assertThat(dataset).isNotNull();
    assertThat(dataset.getLocation()).isEqualTo("us-east1");
    assertThat(dataset.getDescription()).isEqualTo("foo");
    bigquery.delete(datasetId);
  }

  @Test
  public void testDropDatabase() {
    String database =
        String.format("drop_database_test_%s_%s", System.currentTimeMillis(), System.nanoTime());
    DatasetId datasetId = DatasetId.of(database);
    bigquery.create(Dataset.newBuilder(datasetId).build());
    spark.sql("DROP DATABASE " + database + ";");
    Dataset dataset = bigquery.getDataset(datasetId);
    assertThat(dataset).isNull();
  }

  @Test
  public void testCatalogInitializationWithProject() {
    spark
        .conf()
        .set("spark.sql.catalog.public_catalog", "com.google.cloud.spark.bigquery.BigQueryCatalog");
    spark.conf().set("spark.sql.catalog.public_catalog.project", "bigquery-public-data");

    List<Row> rows = spark.sql("SHOW DATABASES IN public_catalog").collectAsList();
    List<String> databaseNames =
        rows.stream().map(row -> row.getString(0)).collect(Collectors.toList());
    assertThat(databaseNames).contains("samples");

    List<Row> data =
        spark.sql("SELECT * FROM public_catalog.samples.shakespeare LIMIT 10").collectAsList();
    assertThat(data).hasSize(10);
  }

  @Test
  public void testCreateCatalogWithLocation() throws Exception {
    String database = String.format("create_db_with_location_%s", System.nanoTime());
    DatasetId datasetId = DatasetId.of(database);
    spark
        .conf()
        .set(
            "spark.sql.catalog.test_location_catalog",
            "com.google.cloud.spark.bigquery.BigQueryCatalog");
    spark.conf().set("spark.sql.catalog.test_location_catalog.bigquery_location", "EU");
    spark.sql("CREATE DATABASE test_location_catalog." + database);
    Dataset dataset = bigquery.getDataset(datasetId);
    assertThat(dataset).isNotNull();
    assertThat(dataset.getLocation()).isEqualTo("EU");
    bigquery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
  }

  @Test
  public void testCreateTableAsSelectWithProjectAndLocation() {
    String database = String.format("ctas_db_with_location_%s", System.nanoTime());
    String newTable = "ctas_table_from_public";
    DatasetId datasetId = DatasetId.of(database);
    spark
        .conf()
        .set("spark.sql.catalog.public_catalog", "com.google.cloud.spark.bigquery.BigQueryCatalog");
    spark.conf().set("spark.sql.catalog.public_catalog.projectId", "bigquery-public-data");
    spark
        .conf()
        .set(
            "spark.sql.catalog.test_catalog_as_select",
            "com.google.cloud.spark.bigquery.BigQueryCatalog");
    spark.conf().set("spark.sql.catalog.test_catalog_as_select.bigquery_location", "EU");
    spark.sql("CREATE DATABASE test_catalog_as_select." + database);
    spark.sql(
        "CREATE TABLE test_catalog_as_select."
            + database
            + "."
            + newTable
            + " AS SELECT * FROM public_catalog.samples.shakespeare LIMIT 10");
    Dataset dataset = bigquery.getDataset(datasetId);
    assertThat(dataset).isNotNull();
    assertThat(dataset.getLocation()).isEqualTo("EU");
    Table table = bigquery.getTable(TableId.of(datasetId.getDataset(), newTable));
    assertThat(table).isNotNull();
    bigquery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
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
