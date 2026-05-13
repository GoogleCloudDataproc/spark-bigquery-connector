/*
 * Copyright 2024 Google Inc. All Rights Reserved.
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
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import java.io.File;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

public class OpenLineageIntegrationTestBase {

  protected SparkBigQueryIntegrationTestRunner testRunner =
      new InMemorySparkBigQueryIntegrationTestRunner();
  @ClassRule public static TestDataset testDataset = new TestDataset();

  protected String testTable;

  @ClassRule public static CustomSessionFactory sessionFactory = new CustomSessionFactory();

  protected static class CustomSessionFactory extends ExternalResource {
    SparkSession spark;
    File lineageFile;

    @Override
    protected void before() throws Throwable {
      lineageFile = File.createTempFile("openlineage_test_" + System.nanoTime(), ".log");
      lineageFile.deleteOnExit();
      spark =
          SparkSession.builder()
              .master("local")
              .appName("openlineage_test_bigquery_connector")
              .config("spark.ui.enabled", "false")
              .config("spark.default.parallelism", 2)
              .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
              .config("spark.openlineage.transport.type", "file")
              .config("spark.openlineage.transport.location", lineageFile.getAbsolutePath())
              .getOrCreate();
      spark.sparkContext().setLogLevel("WARN");
    }
  }

  @Before
  public void createTestTable() {
    testTable = "test_" + System.nanoTime();
  }

  @After
  public void deleteTestTable() throws Exception {
    BigQuery bigquery = IntegrationTestUtils.getBigquery();
    Table table = bigquery.getTable(TableId.of(testDataset.testDataset, testTable));
    if (table != null) {
      table.delete();
    }
  }

  // =========================================================================
  // SCENARIO: OpenLineage Spark agent event logging checks
  // =========================================================================

  protected static JsonObject openLineageApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String scenario = parameters.getOrDefault("scenario", "STANDARD");
    String temporaryGcsBucket = parameters.get("temporaryGcsBucket");
    String lineageFilePath = parameters.get("lineageFilePath");
    java.io.File lineageFile = new java.io.File(lineageFilePath);
    if (lineageFile.exists()) {
      lineageFile.delete();
    }

    SparkSession spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("openlineage_test_bigquery_connector")
            .config("spark.ui.enabled", "false")
            .config("spark.default.parallelism", 2)
            .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
            .config("spark.openlineage.transport.type", "file")
            .config("spark.openlineage.transport.location", lineageFilePath)
            .getOrCreate();

    try {
      // E2E Warm-up query to initialize OpenLineage background agent listener
      spark.sql("SELECT 1").collect();
      Thread.sleep(500);

      String fullTableName = TestConstants.PROJECT_ID + "." + testDataset + "." + testTable;

      if ("STANDARD".equals(scenario)) {
        Dataset<Row> readDF =
            spark.read().format("bigquery").option("table", TestConstants.SHAKESPEARE_TABLE).load();
        readDF.createOrReplaceTempView("words");

        Dataset<Row> writeDF =
            spark.sql("SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word");
        writeDF
            .write()
            .format("bigquery")
            .mode(org.apache.spark.sql.SaveMode.Append)
            .option("table", fullTableName)
            .option("temporaryGcsBucket", temporaryGcsBucket)
            .option("writeMethod", "direct")
            .save();

      } else if ("QUERY".equals(scenario)) {
        Dataset<Row> readDF =
            spark
                .read()
                .format("bigquery")
                .option("viewsEnabled", true)
                .option("materializationDataset", testDataset)
                .option("query", "SELECT * FROM `bigquery-public-data.samples.shakespeare`")
                .load();
        readDF.createOrReplaceTempView("words");

        Dataset<Row> writeDF =
            spark.sql("SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word");
        writeDF
            .write()
            .format("bigquery")
            .mode(org.apache.spark.sql.SaveMode.Append)
            .option("table", fullTableName)
            .option("temporaryGcsBucket", temporaryGcsBucket)
            .option("writeMethod", "direct")
            .save();
      }

      // Flush and parse OpenLineage logs
      // Poll for up to 15 seconds until the lineage file contains logs E2E
      IntegrationTestUtils.pollUntil(
          () -> {
            try (java.util.Scanner scanner = new java.util.Scanner(lineageFile)) {
              return scanner.hasNextLine();
            } catch (Exception e) {
              return false;
            }
          },
          15);

      System.out.println("=== DEBUG RAW LINEAGE FILE START ===");
      try (java.util.Scanner scanner = new java.util.Scanner(lineageFile)) {
        while (scanner.hasNextLine()) {
          System.out.println("=== LINEAGE LINE: " + scanner.nextLine());
        }
      }
      System.out.println("=== DEBUG RAW LINEAGE FILE END ===");

      boolean hasInputEvent = false;
      boolean hasOutputEvent = false;

      try (java.util.Scanner scanner = new java.util.Scanner(lineageFile)) {
        while (scanner.hasNextLine()) {
          String line = scanner.nextLine();
          org.json.JSONObject event = new org.json.JSONObject(line);

          if (event.has("inputs") && !event.getJSONArray("inputs").isEmpty()) {
            String inputName =
                ((org.json.JSONObject) event.getJSONArray("inputs").get(0)).getString("name");
            boolean match =
                inputName
                    .trim()
                    .toLowerCase()
                    .contains(TestConstants.SHAKESPEARE_TABLE.trim().toLowerCase());
            System.out.println(
                "=== DEBUG READ INPUT NAME: ["
                    + inputName
                    + "], expected: ["
                    + TestConstants.SHAKESPEARE_TABLE
                    + "], MATCH: "
                    + match);
            if (match) {
              hasInputEvent = true;
            }
          }

          if (event.has("outputs") && !event.getJSONArray("outputs").isEmpty()) {
            String outputName =
                ((org.json.JSONObject) event.getJSONArray("outputs").get(0)).getString("name");
            boolean match =
                outputName.trim().toLowerCase().contains(fullTableName.trim().toLowerCase());
            System.out.println(
                "=== DEBUG READ OUTPUT NAME: ["
                    + outputName
                    + "], expected: ["
                    + fullTableName
                    + "], MATCH: "
                    + match);
            if (match) {
              hasOutputEvent = true;
            }
          }
        }
      }

      JsonObject result = new JsonObject();
      result.addProperty("status", "success");
      result.addProperty("hasInputEvent", hasInputEvent);
      result.addProperty("hasOutputEvent", hasOutputEvent);
      return result;
    } finally {
    }
  }

  @Test
  public void testLineageEvent() throws Exception {
    JsonObject result =
        testRunner.run(
            OpenLineageIntegrationTestBase::openLineageApp,
            testDataset.testDataset,
            testTable,
            ImmutableMap.of(
                "scenario",
                "STANDARD",
                "lineageFilePath",
                sessionFactory.lineageFile.getAbsolutePath(),
                "temporaryGcsBucket",
                TestConstants.TEMPORARY_GCS_BUCKET));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("hasInputEvent").getAsBoolean()).isTrue();
    assertThat(result.get("hasOutputEvent").getAsBoolean()).isTrue();
  }

  @Test
  public void testLineageEventWithQueryInput() throws Exception {
    JsonObject result =
        testRunner.run(
            OpenLineageIntegrationTestBase::openLineageApp,
            testDataset.testDataset,
            testTable,
            ImmutableMap.of(
                "scenario",
                "QUERY",
                "lineageFilePath",
                sessionFactory.lineageFile.getAbsolutePath(),
                "temporaryGcsBucket",
                TestConstants.TEMPORARY_GCS_BUCKET));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("hasInputEvent").getAsBoolean()).isTrue();
    assertThat(result.get("hasOutputEvent").getAsBoolean()).isTrue();
  }
}
