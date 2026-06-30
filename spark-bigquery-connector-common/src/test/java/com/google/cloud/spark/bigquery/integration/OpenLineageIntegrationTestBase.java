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
import java.nio.file.Files;
import java.util.Map;
import java.util.Scanner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONObject;
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
          IntegrationTestUtils.createSparkSessionBuilder("openlineage_test_bigquery_connector")
              .config("spark.ui.enabled", "false")
              .config("spark.default.parallelism", 2)
              .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
              .config("spark.openlineage.transport.type", "file")
              .config("spark.openlineage.transport.location", lineageFile.getAbsolutePath())
              .getOrCreate()
              .newSession();
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

  @SuppressWarnings({"resource", "java:S2925"})
  protected static JsonObject openLineageApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String scenario = parameters.getOrDefault("scenario", "STANDARD");
    String temporaryGcsBucket = parameters.get("temporaryGcsBucket");
    String lineageFilePath = parameters.get("lineageFilePath");
    File lineageFile = new File(lineageFilePath);
    Files.deleteIfExists(lineageFile.toPath());

    try {
      SparkSession.active().stop();
    } catch (Exception ignored) {
      // Ignored
    }
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();

    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("openlineage_test_bigquery_connector")
            .config("spark.ui.enabled", "false")
            .config("spark.default.parallelism", 2)
            .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
            .config("spark.openlineage.transport.type", "file")
            .config("spark.openlineage.transport.location", lineageFilePath)
            .getOrCreate()
            .newSession();

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
            .mode(SaveMode.Append)
            .option("table", fullTableName)
            .option("temporaryGcsBucket", temporaryGcsBucket)
            .option("writeMethod", "direct")
            .save();
      }

      // Flush and parse OpenLineage logs
      // Poll for up to 45 seconds until the lineage file contains logs E2E
      IntegrationTestUtils.pollUntil(
          () -> {
            try (Scanner scanner = new Scanner(lineageFile, "UTF-8")) {
              while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                try {
                  JSONObject event = new JSONObject(line);
                  if (event.has("outputs") && !event.getJSONArray("outputs").isEmpty()) {
                    JSONArray outputs = event.getJSONArray("outputs");
                    for (int i = 0; i < outputs.length(); i++) {
                      String outputName = outputs.getJSONObject(i).getString("name");
                      if (outputName
                          .trim()
                          .toLowerCase()
                          .contains(testTable.trim().toLowerCase())) {
                        return true;
                      }
                    }
                  }
                } catch (Exception ignored) {
                  // Ignored
                }
              }
              return false;
            } catch (Exception e) {
              return false;
            }
          },
          45);

      boolean hasLinkEvent = false;

      try (Scanner scanner = new Scanner(lineageFile, "UTF-8")) {
        while (scanner.hasNextLine()) {
          String line = scanner.nextLine();
          try {
            JSONObject event = new JSONObject(line);
            boolean hasInput = false;
            boolean hasOutput = false;

            if (event.has("inputs") && !event.getJSONArray("inputs").isEmpty()) {
              JSONArray inputs = event.getJSONArray("inputs");
              for (int i = 0; i < inputs.length(); i++) {
                String inputName = inputs.getJSONObject(i).getString("name");
                if (inputName
                    .trim()
                    .toLowerCase()
                    .contains(TestConstants.SHAKESPEARE_TABLE.trim().toLowerCase())) {
                  hasInput = true;
                  break;
                }
              }
            }

            if (event.has("outputs") && !event.getJSONArray("outputs").isEmpty()) {
              JSONArray outputs = event.getJSONArray("outputs");
              for (int i = 0; i < outputs.length(); i++) {
                String outputName = outputs.getJSONObject(i).getString("name");
                if (outputName.trim().toLowerCase().contains(testTable.trim().toLowerCase())) {
                  hasOutput = true;
                  break;
                }
              }
            }

            if (hasInput && hasOutput) {
              hasLinkEvent = true;
              break;
            }
          } catch (Exception ignored) {
            // Ignored
          }
        }
      }

      JsonObject result = new JsonObject();
      result.addProperty("status", "success");
      result.addProperty("hasLinkEvent", hasLinkEvent);
      return result;
    } finally {
      try {
        spark.stop();
      } catch (Exception ignored) {
        // Ignored
      }
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
    assertThat(result.get("hasLinkEvent").getAsBoolean()).isTrue();
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
    assertThat(result.get("hasLinkEvent").getAsBoolean()).isTrue();
  }
}
