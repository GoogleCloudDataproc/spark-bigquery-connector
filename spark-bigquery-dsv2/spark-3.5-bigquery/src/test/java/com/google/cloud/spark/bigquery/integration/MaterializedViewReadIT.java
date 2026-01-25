/*
 * Copyright 2025 Google LLC and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.spark.bigquery.ReadSessionCreator;
import com.google.cloud.spark.bigquery.connector.common.integration.IntegrationTestUtils;
import com.google.cloud.spark.bigquery.connector.common.integration.SparkBigQueryIntegrationTestBase;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MaterializedViewReadIT extends SparkBigQueryIntegrationTestBase {

  private final Logger logger = Logger.getLogger(ReadSessionCreator.class);
  private WriterAppender appender;
  private StringWriter stringWriter;
  private String testDataset;
  private String mvName;
  private TableId sourceTableId;
  private String testServiceAccount;

  @Before
  public void setUp() throws InterruptedException {
    // Set up a custom log appender to capture logs
    stringWriter = new StringWriter();
    appender = new WriterAppender(new SimpleLayout(), stringWriter);
    appender.setThreshold(Level.WARN);
    logger.addAppender(appender);

    // Create a dedicated service account for this test
    String projectId = System.getenv("GOOGLE_CLOUD_PROJECT");
    assertThat(projectId).isNotNull();
    testServiceAccount =
        String.format(
            "mv-test-%s@%s.iam.gserviceaccount.com",
            UUID.randomUUID().toString().substring(0, 8), projectId);
    IntegrationTestUtils.createServiceAccount(testServiceAccount);

    // Create a temporary dataset and grant the new SA the BQ User role
    // This provides permissions to run jobs (for the fallback) but not to read table data directly
    testDataset = "mv_read_it_" + System.nanoTime();
    IntegrationTestUtils.createDataset(testDataset);
    IntegrationTestUtils.grantServiceAccountRole(
        testServiceAccount, "roles/bigquery.user", testDataset);
    IntegrationTestUtils.grantServiceAccountRole(
        testServiceAccount, "roles/bigquery.metadataViewer", testDataset);

    // Create a source table
    sourceTableId = TableId.of(testDataset, "source_table_" + System.nanoTime());
    IntegrationTestUtils.createTable(sourceTableId, "name:string, value:integer", "name,value");

    // Create a Materialized View
    mvName = "test_mv_" + System.nanoTime();
    String createMvSql =
        String.format(
            "CREATE MATERIALIZED VIEW `%s.%s` AS SELECT name, value FROM `%s.%s`",
            testDataset, mvName, testDataset, sourceTableId.getTable());
    QueryJobConfiguration createMvJob = QueryJobConfiguration.newBuilder(createMvSql).build();
    bigquery.create(JobInfo.of(createMvJob)).waitFor();
  }

  @After
  public void tearDown() {
    logger.removeAppender(appender);
    try {
      if (testDataset != null) {
        IntegrationTestUtils.deleteDatasetAndTables(testDataset);
      }
    } finally {
      if (testServiceAccount != null) {
        IntegrationTestUtils.deleteServiceAccount(testServiceAccount);
      }
    }
  }

  @Test
  public void testReadMaterializedView_lackingPermission_logsWarningAndFallsBack() {
    // This test confirms that when reading a Materialized View with a service account
    // that lacks `bigquery.tables.getData` permission, the connector:
    // 1. Logs a specific WARN message indicating the permission issue and the fallback.
    // 2. Successfully reads the data by falling back to materializing the view's query.

    // Arrange: Use the dedicated service account with insufficient permissions for a direct read.
    String mvToRead = testDataset + "." + mvName;

    // Act: Read the materialized view, impersonating the test service account
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("viewsEnabled", "true") // Required to read any kind of view
            .option("impersonationServiceAccount", testServiceAccount)
            .load(mvToRead);

    List<Row> result = df.collectAsList();

    // Assert
    // 1. Assert that the read was successful via fallback
    assertThat(result).hasSize(2);
    List<String> names = result.stream().map(row -> row.getString(0)).collect(Collectors.toList());
    assertThat(names).containsExactlyElementsIn(Arrays.asList("name1", "name2"));

    // 2. Assert that the specific warning was logged
    String logOutput = stringWriter.toString();
    assertThat(logOutput).contains("Failed to initiate a direct read from Materialized View");
    assertThat(logOutput)
        .contains("The service account likely lacks 'bigquery.tables.getData' permission");
    assertThat(logOutput).contains("Falling back to re-executing the view's underlying query");
  }
}
