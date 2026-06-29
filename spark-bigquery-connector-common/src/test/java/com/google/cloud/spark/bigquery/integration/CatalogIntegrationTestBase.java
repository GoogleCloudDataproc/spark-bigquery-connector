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
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CatalogIntegrationTestBase {

  protected SparkBigQueryIntegrationTestRunner testRunner =
      new InMemorySparkBigQueryIntegrationTestRunner();

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
    Table customTable = bigquery.getTable(TableId.of(testDataset.testDataset, testTable));
    if (customTable != null) {
      customTable.delete();
    }
  }

  // =========================================================================
  // SCENARIO: Spark Catalog DDL & DML operations
  // =========================================================================

  @SuppressWarnings("resource")
  protected static JsonObject catalogApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String scenario = parameters.getOrDefault("scenario", "CREATE_TABLE_DEFAULT");
    String dataset = parameters.getOrDefault("dataset", "default");
    String location = parameters.getOrDefault("location", "US");
    String database = parameters.getOrDefault("database", "test_db");

    SparkSession.Builder builder =
        IntegrationTestUtils.createSparkSessionBuilder("CatalogTestApp")
            .config("spark.sql.legacy.createHiveTableByDefault", "false")
            .config("spark.sql.sources.default", "bigquery")
            .config("spark.datasource.bigquery.writeMethod", "direct")
            .config("spark.sql.defaultCatalog", "bigquery")
            .config(
                "spark.sql.catalog.bigquery", "com.google.cloud.spark.bigquery.BigQueryCatalog");

    SparkSession spark = builder.getOrCreate().newSession();

    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      if (entry.getKey().startsWith("spark.")) {
        spark.conf().set(entry.getKey(), entry.getValue());
      }
    }

    try {
      JsonObject result = new JsonObject();
      result.addProperty("status", "success");

      switch (scenario) {
        case "CREATE_TABLE_DEFAULT":
        case "CREATE_TABLE_CUSTOM":
          spark.sql("CREATE TABLE " + fullTableName(dataset, testTable) + "(id int, data string);");
          break;

        case "CREATE_INSERT_DEFAULT":
        case "CREATE_INSERT_CUSTOM":
          spark.sql("CREATE TABLE " + fullTableName(dataset, testTable) + "(id int, data string);");
          spark.sql(String.format("INSERT INTO `%s`.`%s` VALUES (1, 'foo');", dataset, testTable));
          break;

        case "CTAS_DEFAULT":
        case "CTAS_CUSTOM":
          spark.sql(
              "CREATE TABLE "
                  + fullTableName(dataset, testTable)
                  + " AS SELECT 1 AS id, 'foo' AS data;");
          break;

        case "READ_DIFFERENT_PROJECT":
          List<Row> rowsDifferentProject =
              spark
                  .sql(
                      "SELECT * from `bigquery-public-data`.`samples`.`shakespeare` WHERE word='spark'")
                  .collectAsList();
          result.addProperty("rowCount", rowsDifferentProject.size());
          break;

        case "LIST_NAMESPACES":
          List<Row> databases = spark.sql("SHOW DATABASES").collectAsList();
          List<String> dbs =
              databases.stream().map(row -> row.getString(0)).collect(Collectors.toList());
          result.addProperty("containsDatabase", dbs.contains(database));
          break;

        case "CREATE_NAMESPACE":
          spark.sql("CREATE DATABASE " + database + ";");
          break;

        case "CREATE_NAMESPACE_LOCATION":
          spark.sql(
              "CREATE DATABASE "
                  + database
                  + " COMMENT 'foo' WITH DBPROPERTIES (bigquery_location = '"
                  + location
                  + "');");
          break;

        case "DROP_DATABASE":
          spark.sql("DROP DATABASE " + database + ";");
          break;

        case "CATALOG_INIT_PROJECT":
          IntegrationTestUtils.pollUntil(
              () -> {
                try {
                  return spark.sql("SHOW DATABASES IN public_catalog").collectAsList().stream()
                      .map(row -> row.getString(0))
                      .collect(Collectors.toList())
                      .contains("samples");
                } catch (Exception e) {
                  return false;
                }
              },
              45);

          boolean containsSamples =
              spark.sql("SHOW DATABASES IN public_catalog").collectAsList().stream()
                  .anyMatch(row -> "samples".equals(row.getString(0)));

          List<Row> data =
              spark
                  .sql("SELECT * FROM public_catalog.samples.shakespeare LIMIT 10")
                  .collectAsList();

          result.addProperty("containsSamples", containsSamples);
          result.addProperty("limitCount", data.size());
          break;

        case "CATALOG_EU_LOCATION":
          IntegrationTestUtils.pollUntil(
              () -> {
                try {
                  spark.sql("SHOW DATABASES IN test_location_catalog").collect();
                  return true;
                } catch (Exception e) {
                  return false;
                }
              },
              45);

          spark.sql("CREATE DATABASE test_location_catalog." + database);
          break;

        case "CTAS_PROJECT_LOCATION":
          IntegrationTestUtils.pollUntil(
              () -> {
                try {
                  spark.sql("SHOW DATABASES IN test_catalog_as_select").collect();
                  return true;
                } catch (Exception e) {
                  return false;
                }
              },
              45);

          spark.sql("CREATE DATABASE test_catalog_as_select." + database);
          IntegrationTestUtils.pollUntil(
              () -> {
                try {
                  return spark.sql("SHOW DATABASES IN test_catalog_as_select").collectAsList()
                      .stream()
                      .anyMatch(row -> database.equals(row.getString(0)));
                } catch (Exception e) {
                  return false;
                }
              },
              45);

          spark.sql(
              "CREATE TABLE test_catalog_as_select."
                  + database
                  + "."
                  + testTable
                  + " AS SELECT * FROM public_catalog.samples.shakespeare LIMIT 10");
          break;

        default:
          break;
      }

      return result;
    } finally {
      try {
        for (String key : parameters.keySet()) {
          if (key.startsWith("spark.")) {
            spark.conf().unset(key);
          }
        }
      } catch (java.util.NoSuchElementException ignored) {
      }
    }
  }

  private static String fullTableName(String dataset, String testTable) {
    return DEFAULT_NAMESPACE.equals(dataset)
        ? "`" + testTable + "`"
        : "`" + dataset + "`.`" + testTable + "`";
  }

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
  public void testCreateTableInDefaultNamespace() throws Exception {
    JsonObject result =
        testRunner.run(
            CatalogIntegrationTestBase::catalogApp,
            testDataset.testDataset,
            testTable,
            ImmutableMap.of("scenario", "CREATE_TABLE_DEFAULT", "dataset", DEFAULT_NAMESPACE));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    Table table = bigquery.getTable(TableId.of(DEFAULT_NAMESPACE, testTable));
    assertThat(table).isNotNull();
    assertThat(selectCountStarFrom(DEFAULT_NAMESPACE, testTable)).isEqualTo(0L);
  }

  @Test
  public void testCreateTableInCustomNamespace() throws Exception {
    JsonObject result =
        testRunner.run(
            CatalogIntegrationTestBase::catalogApp,
            testDataset.testDataset,
            testTable,
            ImmutableMap.of("scenario", "CREATE_TABLE_CUSTOM", "dataset", testDataset.testDataset));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    Table table = bigquery.getTable(TableId.of(testDataset.testDataset, testTable));
    assertThat(table).isNotNull();
    assertThat(selectCountStarFrom(testDataset.testDataset, testTable)).isEqualTo(0L);
  }

  @Test
  public void testCreateTableAndInsertInDefaultNamespace() throws Exception {
    JsonObject result =
        testRunner.run(
            CatalogIntegrationTestBase::catalogApp,
            testDataset.testDataset,
            testTable,
            ImmutableMap.of("scenario", "CREATE_INSERT_DEFAULT", "dataset", DEFAULT_NAMESPACE));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    Table table = bigquery.getTable(TableId.of(DEFAULT_NAMESPACE, testTable));
    assertThat(table).isNotNull();
    assertThat(selectCountStarFrom(DEFAULT_NAMESPACE, testTable)).isEqualTo(1L);
  }

  @Test
  public void testCreateTableAndInsertInCustomNamespace() throws Exception {
    JsonObject result =
        testRunner.run(
            CatalogIntegrationTestBase::catalogApp,
            testDataset.testDataset,
            testTable,
            ImmutableMap.of(
                "scenario", "CREATE_INSERT_CUSTOM", "dataset", testDataset.testDataset));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    Table table = bigquery.getTable(TableId.of(testDataset.testDataset, testTable));
    assertThat(table).isNotNull();
    assertThat(selectCountStarFrom(testDataset.testDataset, testTable)).isEqualTo(1L);
  }

  @Test
  public void testCreateTableAsSelectInDefaultNamespace() throws Exception {
    JsonObject result =
        testRunner.run(
            CatalogIntegrationTestBase::catalogApp,
            testDataset.testDataset,
            testTable,
            ImmutableMap.of("scenario", "CTAS_DEFAULT", "dataset", DEFAULT_NAMESPACE));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    Table table = bigquery.getTable(TableId.of(DEFAULT_NAMESPACE, testTable));
    assertThat(table).isNotNull();
    assertThat(selectCountStarFrom(DEFAULT_NAMESPACE, testTable)).isEqualTo(1L);
  }

  @Test
  public void testCreateTableAsSelectInCustomNamespace() throws Exception {
    JsonObject result =
        testRunner.run(
            CatalogIntegrationTestBase::catalogApp,
            testDataset.testDataset,
            testTable,
            ImmutableMap.of("scenario", "CTAS_CUSTOM", "dataset", testDataset.testDataset));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    Table table = bigquery.getTable(TableId.of(testDataset.testDataset, testTable));
    assertThat(table).isNotNull();
    assertThat(selectCountStarFrom(testDataset.testDataset, testTable)).isEqualTo(1L);
  }

  @Test
  public void testReadFromDifferentBigQueryProject() throws Exception {
    JsonObject result =
        testRunner.run(
            CatalogIntegrationTestBase::catalogApp,
            testDataset.testDataset,
            testTable,
            ImmutableMap.of("scenario", "READ_DIFFERENT_PROJECT"));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("rowCount").getAsInt()).isEqualTo(9);
  }

  @Test
  public void testListNamespaces() throws Exception {
    String database =
        String.format("show_databases_test_%s_%s", System.currentTimeMillis(), System.nanoTime());
    DatasetId datasetId = DatasetId.of(database);
    bigquery.create(Dataset.newBuilder(datasetId).build());
    try {
      JsonObject result =
          testRunner.run(
              CatalogIntegrationTestBase::catalogApp,
              testDataset.testDataset,
              testTable,
              ImmutableMap.of("scenario", "LIST_NAMESPACES", "database", database));

      assertThat(result.get("status").getAsString()).isEqualTo("success");
      assertThat(result.get("containsDatabase").getAsBoolean()).isTrue();
    } finally {
      bigquery.delete(datasetId);
    }
  }

  @Test
  public void testCreateNamespace() throws Exception {
    String database =
        String.format("create_database_test_%s_%s", System.currentTimeMillis(), System.nanoTime());
    DatasetId datasetId = DatasetId.of(database);
    try {
      JsonObject result =
          testRunner.run(
              CatalogIntegrationTestBase::catalogApp,
              testDataset.testDataset,
              testTable,
              ImmutableMap.of("scenario", "CREATE_NAMESPACE", "database", database));

      assertThat(result.get("status").getAsString()).isEqualTo("success");
      Dataset dataset = bigquery.getDataset(datasetId);
      assertThat(dataset).isNotNull();
    } finally {
      bigquery.delete(datasetId);
    }
  }

  @Test
  public void testCreateNamespaceWithLocation() throws Exception {
    String database =
        String.format("create_database_test_%s_%s", System.currentTimeMillis(), System.nanoTime());
    DatasetId datasetId = DatasetId.of(database);
    try {
      JsonObject result =
          testRunner.run(
              CatalogIntegrationTestBase::catalogApp,
              testDataset.testDataset,
              testTable,
              ImmutableMap.of(
                  "scenario",
                  "CREATE_NAMESPACE_LOCATION",
                  "database",
                  database,
                  "location",
                  "us-east1"));

      assertThat(result.get("status").getAsString()).isEqualTo("success");
      Dataset dataset = bigquery.getDataset(datasetId);
      assertThat(dataset).isNotNull();
      assertThat(dataset.getLocation()).isEqualTo("us-east1");
      assertThat(dataset.getDescription()).isEqualTo("foo");
    } finally {
      bigquery.delete(datasetId);
    }
  }

  @Test
  public void testDropDatabase() throws Exception {
    String database =
        String.format("drop_database_test_%s_%s", System.currentTimeMillis(), System.nanoTime());
    DatasetId datasetId = DatasetId.of(database);
    bigquery.create(Dataset.newBuilder(datasetId).build());
    try {
      JsonObject result =
          testRunner.run(
              CatalogIntegrationTestBase::catalogApp,
              testDataset.testDataset,
              testTable,
              ImmutableMap.of("scenario", "DROP_DATABASE", "database", database));

      assertThat(result.get("status").getAsString()).isEqualTo("success");
      Dataset dataset = bigquery.getDataset(datasetId);
      assertThat(dataset).isNull();
    } finally {
      bigquery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
    }
  }

  @Test
  public void testCatalogInitializationWithProject() throws Exception {
    JsonObject result =
        testRunner.run(
            CatalogIntegrationTestBase::catalogApp,
            testDataset.testDataset,
            testTable,
            ImmutableMap.of(
                "scenario",
                "CATALOG_INIT_PROJECT",
                "spark.sql.catalog.public_catalog",
                "com.google.cloud.spark.bigquery.BigQueryCatalog",
                "spark.sql.catalog.public_catalog.projectId",
                "bigquery-public-data"));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("containsSamples").getAsBoolean()).isTrue();
    assertThat(result.get("limitCount").getAsInt()).isEqualTo(10);
  }

  @Test
  public void testCreateCatalogWithLocation() throws Exception {
    String database = String.format("create_db_with_location_%s", System.nanoTime());
    DatasetId datasetId = DatasetId.of(database);
    try {
      JsonObject result =
          testRunner.run(
              CatalogIntegrationTestBase::catalogApp,
              testDataset.testDataset,
              testTable,
              ImmutableMap.of(
                  "scenario",
                  "CATALOG_EU_LOCATION",
                  "database",
                  database,
                  "spark.sql.catalog.test_location_catalog",
                  "com.google.cloud.spark.bigquery.BigQueryCatalog",
                  "spark.sql.catalog.test_location_catalog.bigquery_location",
                  "EU"));

      assertThat(result.get("status").getAsString()).isEqualTo("success");
      Dataset dataset = bigquery.getDataset(datasetId);
      assertThat(dataset).isNotNull();
      assertThat(dataset.getLocation()).isEqualTo("EU");
    } finally {
      bigquery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
    }
  }

  @Test
  public void testCreateTableAsSelectWithProjectAndLocation() throws Exception {
    String database = String.format("ctas_db_with_location_%s", System.nanoTime());
    DatasetId datasetId = DatasetId.of(database);
    try {
      JsonObject result =
          testRunner.run(
              CatalogIntegrationTestBase::catalogApp,
              testDataset.testDataset,
              testTable,
              ImmutableMap.of(
                  "scenario",
                  "CTAS_PROJECT_LOCATION",
                  "database",
                  database,
                  "spark.sql.catalog.public_catalog",
                  "com.google.cloud.spark.bigquery.BigQueryCatalog",
                  "spark.sql.catalog.public_catalog.projectId",
                  "bigquery-public-data",
                  "spark.sql.catalog.test_catalog_as_select",
                  "com.google.cloud.spark.bigquery.BigQueryCatalog",
                  "spark.sql.catalog.test_catalog_as_select.bigquery_location",
                  "EU"));

      assertThat(result.get("status").getAsString()).isEqualTo("success");
      Dataset dataset = bigquery.getDataset(datasetId);
      assertThat(dataset).isNotNull();
      assertThat(dataset.getLocation()).isEqualTo("EU");
      Table table = bigquery.getTable(TableId.of(datasetId.getDataset(), testTable));
      assertThat(table).isNotNull();
    } finally {
      bigquery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
    }
  }
}
