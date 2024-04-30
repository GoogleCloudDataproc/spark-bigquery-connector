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

import com.google.cloud.spark.bigquery.integration.SparkBigQueryIntegrationTestBase.TestDataset;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

public class OpenLineageIntegrationTestBase {
  @ClassRule
  public static TestDataset testDataset = new SparkBigQueryIntegrationTestBase.TestDataset();

  @ClassRule public static CustomSessionFactory sessionFactory = new CustomSessionFactory();

  protected SparkSession spark;
  protected String testTable;
  protected File lineageFile;

  public OpenLineageIntegrationTestBase() {
    this.spark = sessionFactory.spark;
    this.lineageFile = sessionFactory.lineageFile;
  }

  @Before
  public void createTestTable() {
    testTable = "test_" + System.nanoTime();
  }

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
              .config("spark.default.parallelism", 20)
              .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
              .config("spark.openlineage.transport.type", "file")
              .config("spark.openlineage.transport.location", lineageFile.getAbsolutePath())
              .getOrCreate();
      spark.sparkContext().setLogLevel("WARN");
    }
  }

  private List<JSONObject> parseEventLogs(File file) throws Exception {
    List<JSONObject> eventList;
    try (Scanner scanner = new Scanner(file)) {
      eventList = new ArrayList<>();
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        JSONObject event = new JSONObject(line);
        if (!event.getJSONArray("inputs").isEmpty() && !event.getJSONArray("outputs").isEmpty()) {
          eventList.add(event);
        }
      }
    }
    return eventList;
  }

  private String getFieldName(JSONObject event, String field) {
    JSONObject eventField = (JSONObject) event.getJSONArray(field).get(0);
    return eventField.getString("name");
  }

  @Test
  public void testLineageEvent() throws Exception {
    String fullTableName = testDataset.toString() + "." + testTable;
    Dataset<Row> readDF =
        spark.read().format("bigquery").option("table", TestConstants.SHAKESPEARE_TABLE).load();
    readDF.createOrReplaceTempView("words");
    Dataset<Row> writeDF =
        spark.sql("SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word");
    writeDF
        .write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("table", fullTableName)
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("writeMethod", "direct")
        .save();
    List<JSONObject> eventList = parseEventLogs(lineageFile);
    assertThat(eventList)
        .isNotEmpty(); // check if there is at least one event with both input and output
    eventList.forEach(
        (event) -> { // check if each of these events have the correct input and output
          assertThat(getFieldName(event, "inputs")).matches(TestConstants.SHAKESPEARE_TABLE);
          assertThat(getFieldName(event, "outputs")).matches(fullTableName);
        });
  }
}
