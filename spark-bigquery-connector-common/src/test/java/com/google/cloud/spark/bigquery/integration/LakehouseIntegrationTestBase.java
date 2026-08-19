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

import com.google.cloud.biglake.v1.IcebergCatalog;
import com.google.cloud.biglake.v1.IcebergCatalogServiceClient;
import com.google.cloud.biglake.v1.IcebergCatalogServiceSettings;
import com.google.cloud.biglake.v1.ProjectName;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LakehouseIntegrationTestBase {
  // BigQuery query interop for BigLake Lakehouse Iceberg tables (referencing a
  // catalog.namespace.table as a 4-part BigQuery name and running DML/reads on
  // it) relies on the router's IRC table-name parsing. That parsing is resolved
  // for a concrete single region via the fleet-wide launch, but is not yet
  // resolved for multi-region locations such as "US" (tracked in b/524657483).
  // The test therefore runs in a single region by default; override with
  // BIGQUERY_LAKEHOUSE_TEST_LOCATION if needed.
  private static final String LOCATION =
      Optional.ofNullable(System.getenv("BIGQUERY_LAKEHOUSE_TEST_LOCATION")).orElse("us-central1");

  Storage storage = StorageOptions.newBuilder().build().getService();
  String catalogName;
  String namespace;
  String testTable;

  @Before
  public void createCatalog() throws Exception {
    try (IcebergCatalogServiceClient catalogServiceClient = createIcebergCatalogServiceClient()) {
      catalogName = String.format("sbc%x", System.nanoTime());
      // 1. create test GCS bucket for the Iceberg catalog
      storage.create(BucketInfo.newBuilder(catalogName).setLocation(LOCATION).build());

      // 2. create the BigLake Iceberg catalog
      IcebergCatalog icebergCatalog =
          catalogServiceClient.createIcebergCatalog(
              ProjectName.of(TestConstants.PROJECT_ID).toString(),
              IcebergCatalog.newBuilder()
                  .setCatalogType(IcebergCatalog.CatalogType.CATALOG_TYPE_GCS_BUCKET)
                  .build(),
              catalogName);
      assertThat(icebergCatalog).isNotNull();
    }
  }

  @After
  public void cleanup() throws Exception {
    // createCatalog() was skipped (feature not enabled) or failed before creating
    // anything; nothing to clean up.
    if (catalogName == null) {
      return;
    }
    // Best-effort cleanup: never throw from teardown so we do not mask the real
    // test result. Drop the table/namespace via the Iceberg REST catalog (the same
    // path used to create them), then delete the backing GCS bucket.
    if (namespace != null) {
      try (SparkSession spark = createSparkSessionWithLakehouseCatalog("cleanup", catalogName)) {
        if (testTable != null) {
          spark.sql(String.format("DROP TABLE IF EXISTS %s.%s", namespace, testTable));
        }
        spark.sql(String.format("DROP NAMESPACE IF EXISTS %s", namespace));
      } catch (Exception e) {
        System.err.println("Lakehouse test cleanup: failed to drop table/namespace: " + e);
      }
    }
    try {
      storage.list(catalogName).iterateAll().forEach(Blob::delete);
      storage.delete(catalogName);
    } catch (Exception e) {
      System.err.println("Lakehouse test cleanup: failed to delete GCS bucket: " + e);
    }
    // TODO(b/524657483): delete the Iceberg catalog itself. The pinned
    // google-cloud-biglake (v1 IcebergCatalogServiceClient) exposes no catalog
    // delete method, and the public BigLake v1 REST surface only offers IAM
    // methods at the catalog level, so the (now empty) catalog resource is left
    // behind. Delete via `gcloud biglake iceberg catalogs delete`, or bump
    // google-cloud-biglake to a version that provides deleteIcebergCatalog().
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
          TestConstants.PROJECT_ID,
          catalogName,
          namespace,
          testTable);
      df =
          spark
              .read()
              .format("bigquery")
              .load(
                  String.format(
                      "%s.%s.%s.%s", TestConstants.PROJECT_ID, catalogName, namespace, testTable));

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
            .config(
                "spark.sql.catalog.lakehouse.uri",
                "https://biglake.googleapis.com/iceberg/v1/restcatalog")
            .config("spark.sql.catalog.lakehouse.warehouse", "gs://" + catalogName)
            .config(
                "spark.sql.catalog.lakehouse.header.x-goog-user-project", TestConstants.PROJECT_ID)
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
      if (customEndpoint.startsWith("https://")) {
        settingsBuilder = IcebergCatalogServiceSettings.newHttpJsonBuilder();
      }
      settingsBuilder.setEndpoint(customEndpoint);
    }
    return IcebergCatalogServiceClient.create(settingsBuilder.build());
  }
}
