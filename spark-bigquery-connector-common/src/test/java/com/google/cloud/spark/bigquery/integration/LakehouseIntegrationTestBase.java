/*
 * Copyright 2026 Google Inc. All Rights Reserved.
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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.biglake.v1.IcebergCatalog;
import com.google.cloud.biglake.v1.IcebergCatalogServiceClient;
import com.google.cloud.biglake.v1.IcebergCatalogServiceSettings;
import com.google.cloud.biglake.v1.ProjectName;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LakehouseIntegrationTestBase {
  private static final String BIGLAKE_REST_CATALOG_PATH = "/iceberg/v1/restcatalog";
  private static final String DEFAULT_BIGLAKE_API_ENDPOINT = "https://biglake.googleapis.com";
  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";
  private static final int CATALOG_DELETE_TIMEOUT_MILLIS = 30_000;
  private static final String PROJECT_ID =
      Preconditions.checkNotNull(
          System.getenv("GOOGLE_CLOUD_PROJECT"),
          "Please set the GOOGLE_CLOUD_PROJECT env variable");

  // BigQuery query interop for BigLake Lakehouse Iceberg tables (referencing a
  // catalog.namespace.table as a 4-part BigQuery name and running DML/reads on
  // it) relies on the router's IRC table-name parsing. That parsing is resolved
  // for a concrete single region via the fleet-wide launch, but is not yet
  // resolved for multi-region locations such as "US" (tracked in b/524657483).
  // The test therefore runs in a single region by default; override with
  // BIGQUERY_LAKEHOUSE_TEST_LOCATION if needed.
  private static final String LOCATION =
      Optional.ofNullable(System.getenv("BIGQUERY_LAKEHOUSE_TEST_LOCATION")).orElse("us-central1");

  private final Storage storage = StorageOptions.newBuilder().build().getService();
  private String catalogName;
  private String namespace;
  private String testTable;
  private boolean bucketCreated;
  private boolean catalogCreated;

  @Before
  public void createCatalog() throws Exception {
    try (IcebergCatalogServiceClient catalogServiceClient = createIcebergCatalogServiceClient()) {
      catalogName = "sbc" + UUID.randomUUID().toString().replace("-", "");
      // 1. create test GCS bucket for the Iceberg catalog
      storage.create(BucketInfo.newBuilder(catalogName).setLocation(LOCATION).build());
      bucketCreated = true;

      // 2. create the BigLake Iceberg catalog
      IcebergCatalog icebergCatalog =
          catalogServiceClient.createIcebergCatalog(
              ProjectName.of(PROJECT_ID).toString(),
              IcebergCatalog.newBuilder()
                  .setCatalogType(IcebergCatalog.CatalogType.CATALOG_TYPE_GCS_BUCKET)
                  .build(),
              catalogName);
      assertThat(icebergCatalog).isNotNull();
      catalogCreated = true;
    }
  }

  @After
  public void cleanup() throws Exception {
    List<Throwable> cleanupFailures = new ArrayList<>();

    // Remove contents before deleting the catalog. JUnit combines teardown exceptions with an
    // existing test failure, so reporting cleanup errors does not hide the original failure.
    if (catalogCreated && namespace != null) {
      try (SparkSession spark = createSparkSessionWithLakehouseCatalog("cleanup", catalogName)) {
        if (testTable != null) {
          spark.sql(String.format("DROP TABLE IF EXISTS %s.%s", namespace, testTable));
        }
        spark.sql(String.format("DROP NAMESPACE IF EXISTS %s", namespace));
      } catch (Exception e) {
        cleanupFailures.add(e);
      }
    }

    if (catalogCreated) {
      try {
        deleteIcebergCatalog();
        catalogCreated = false;
      } catch (Exception e) {
        cleanupFailures.add(e);
      }
    }

    // Keep the bucket when catalog deletion fails. That avoids leaving a live catalog pointing at
    // a missing warehouse and gives the failed resource a recoverable state for manual cleanup.
    if (bucketCreated && !catalogCreated) {
      try {
        storage.list(catalogName).iterateAll().forEach(Blob::delete);
        if (!storage.delete(catalogName)) {
          throw new IllegalStateException("GCS bucket was not deleted: " + catalogName);
        }
        bucketCreated = false;
      } catch (Exception e) {
        cleanupFailures.add(e);
      }
    }

    if (!cleanupFailures.isEmpty()) {
      AssertionError cleanupFailure =
          new AssertionError("Lakehouse integration test cleanup failed");
      cleanupFailures.forEach(cleanupFailure::addSuppressed);
      throw cleanupFailure;
    }
  }

  @Test
  public void testReadFromIcebergCatalog() throws Exception {
    namespace = String.format("sbc_%x", System.nanoTime());
    testTable = String.format("shakespeare_%x", System.nanoTime());
    Dataset<Row> df;
    try (SparkSession spark =
        createSparkSessionWithLakehouseCatalog("testReadFromIcebergCatalog", catalogName)) {
      spark.sql("CREATE NAMESPACE " + namespace);
      spark.sql(
          String.format(
              "CREATE TABLE %s.%s (word STRING, word_count INT, corpus STRING, corpus_date INT) "
                  + "USING ICEBERG "
                  + "TBLPROPERTIES ('gcp.biglake.bigquery-dml.enabled' = true)",
              namespace, testTable));
      // Seed the table with deterministic rows via BigQuery DML. The rows are
      // inlined (rather than read from a public dataset) so the query has no
      // cross-location source dependency and can run in a single region.
      IntegrationTestUtils.runQueryInLocation(
          LOCATION,
          "INSERT INTO `%s`.`%s`.`%s`.`%s` (word, word_count, corpus, corpus_date) VALUES "
              + "('spark', 10, 'sonnets', 0), "
              + "('bigquery', 20, 'sonnets', 0), "
              + "('iceberg', 30, 'sonnets', 0);",
          PROJECT_ID,
          catalogName,
          namespace,
          testTable);
      df =
          spark
              .read()
              .format("bigquery")
              .load(String.format("%s.%s.%s.%s", PROJECT_ID, catalogName, namespace, testTable));

      assertThat(df.count()).isEqualTo(3L);
      List<Row> result = df.where("word = 'spark'").collectAsList();
      assertThat(result).hasSize(1);
    }
  }

  private SparkSession createSparkSessionWithLakehouseCatalog(String testName, String catalogName) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local")
            .appName(testName)
            .config("spark.hadoop.google.cloud.appName.v2", testName)
            .config("spark.ui.enabled", "false")
            .config("spark.default.parallelism", 20)
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "rest")
            .config("spark.sql.catalog.lakehouse.uri", getBigLakeRestCatalogUri())
            .config("spark.sql.catalog.lakehouse.warehouse", "gs://" + catalogName)
            .config("spark.sql.catalog.lakehouse.header.x-goog-user-project", PROJECT_ID)
            .config(
                "spark.sql.catalog.lakehouse.rest.auth.type",
                "org.apache.iceberg.gcp.auth.GoogleAuthManager")
            .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
            .config("spark.sql.catalog.lakehouse.rest-metrics-reporting-enabled", "false")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.defaultCatalog", "lakehouse");
    Optional<String> bigqueryApiHttpEndpoint =
        Optional.ofNullable(System.getenv("BIGQUERY_API_HTTP_ENDPOINT"));
    Optional<String> bigqueryStorageApiGrpcEndpoint =
        Optional.ofNullable(System.getenv("BIGQUERY_STORAGE_API_GRPC_ENDPOINT"));
    if (bigqueryApiHttpEndpoint.isPresent()) {
      builder = builder.config("bigQueryHttpEndpoint", bigqueryApiHttpEndpoint.get());
    }
    if (bigqueryStorageApiGrpcEndpoint.isPresent()) {
      builder = builder.config("bigQueryStorageGrpcEndpoint", bigqueryStorageApiGrpcEndpoint.get());
    }
    return builder.getOrCreate();
  }

  private static IcebergCatalogServiceClient createIcebergCatalogServiceClient() throws Exception {
    String customEndpoint = System.getenv("BIGLAKE_API_ENDPOINT");
    IcebergCatalogServiceSettings.Builder settingsBuilder =
        IcebergCatalogServiceSettings.newBuilder();
    if (customEndpoint != null && !customEndpoint.trim().isEmpty()) {
      if (customEndpoint.startsWith("http://") || customEndpoint.startsWith("https://")) {
        settingsBuilder = IcebergCatalogServiceSettings.newHttpJsonBuilder();
      }
      settingsBuilder.setEndpoint(customEndpoint);
    }
    return IcebergCatalogServiceClient.create(settingsBuilder.build());
  }

  private void deleteIcebergCatalog() throws Exception {
    URL deleteUrl =
        new URL(
            String.format(
                "%s/extensions/projects/%s/catalogs/%s",
                getBigLakeRestCatalogUri(), PROJECT_ID, catalogName));
    GoogleCredentials credentials =
        GoogleCredentials.getApplicationDefault()
            .createScoped(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
    credentials.refreshIfExpired();

    HttpURLConnection connection = (HttpURLConnection) deleteUrl.openConnection();
    try {
      connection.setConnectTimeout(CATALOG_DELETE_TIMEOUT_MILLIS);
      connection.setReadTimeout(CATALOG_DELETE_TIMEOUT_MILLIS);
      connection.setRequestMethod("DELETE");
      connection.setRequestProperty(
          "Authorization", "Bearer " + credentials.getAccessToken().getTokenValue());
      connection.setRequestProperty("x-goog-user-project", PROJECT_ID);

      int responseCode = connection.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK
          && responseCode != HttpURLConnection.HTTP_NO_CONTENT
          && responseCode != HttpURLConnection.HTTP_NOT_FOUND) {
        String responseBody;
        InputStream errorStream = connection.getErrorStream();
        if (errorStream == null) {
          responseBody = "";
        } else {
          try (InputStreamReader reader =
              new InputStreamReader(errorStream, StandardCharsets.UTF_8)) {
            responseBody = CharStreams.toString(reader);
          }
        }
        throw new IllegalStateException(
            String.format(
                "Failed to delete Iceberg catalog %s: HTTP %d %s",
                catalogName, responseCode, responseBody));
      }
    } finally {
      connection.disconnect();
    }
  }

  private static String getBigLakeRestCatalogUri() {
    String endpoint = System.getenv("BIGLAKE_API_ENDPOINT");
    if (endpoint == null || endpoint.trim().isEmpty()) {
      endpoint = DEFAULT_BIGLAKE_API_ENDPOINT;
    } else {
      endpoint = endpoint.trim();
      if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
        endpoint = "https://" + endpoint;
      }
    }
    endpoint = endpoint.replaceFirst("/+$", "");
    return endpoint.endsWith(BIGLAKE_REST_CATALOG_PATH)
        ? endpoint
        : endpoint + BIGLAKE_REST_CATALOG_PATH;
  }
}
