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
import com.google.cloud.bigquery.biglake.v1alpha1.MetastoreServiceClient;
import com.google.cloud.bigquery.biglake.v1alpha1.MetastoreServiceSettings;
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
  private final String LOCATION = "US";

  Storage storage = StorageOptions.newBuilder().build().getService();
  String catalogName;

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
    } finally {
      // no-op
    }
  }

  @After
  public void cleanup() throws Exception {
    try (MetastoreServiceClient metastoreServiceClient = createMetastoreServiceClient()) {
      storage.list(catalogName).iterateAll().forEach(Blob::delete);
      storage.delete(catalogName);
      metastoreServiceClient.deleteCatalog(
          String.format("projects/%s/catalogs/%s", TestConstants.PROJECT_ID, catalogName));
    } finally {
      // no-op
    }
  }

  @Test
  public void testReadFromIcebergCatalog() throws Exception {
    String namespace = String.format("sbc_%x", System.nanoTime());
    String testTable = String.format("shakespeare_%x", System.nanoTime());
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
      IntegrationTestUtils.runQueryInLocation(
          LOCATION,
          "INSERT INTO `%s`.`%s`.`%s`.`%s` (word, word_count, corpus, corpus_date) "
              + "SELECT word, word_count, corpus, corpus_date FROM `bigquery-public-data.samples.shakespeare`;",
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
                      "%s.%s.%s.%s", TestConstants.PROJECT_ID, "lakehouse", namespace, testTable));

      List<Row> result = df.where("word='spark'").collectAsList();
      assertThat(result).hasSize(9);
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
//    Optional.ofNullable(System.getenv("BIGQUERY_API_HTTP_ENDPOINT"))
//        .ifPresent(value -> builder.config("bigQueryHttpEndpoint", value));
//    Optional.ofNullable(System.getenv("BIGQUERY_STORAGE_API_GRPC_ENDPOINT"))
//        .ifPresent(value -> builder.config("bigQueryStorageGrpcEndpoint", value));
    return builder.getOrCreate();
  }

  private static IcebergCatalogServiceClient createIcebergCatalogServiceClient() throws Exception {
    String customEndpoint = System.getenv("BIGLAKE_API_ENDPOINT");
    IcebergCatalogServiceSettings.Builder settingsBuilder =
        IcebergCatalogServiceSettings.newBuilder();
    if (customEndpoint != null && !customEndpoint.trim().isEmpty()) {
      settingsBuilder.setEndpoint(customEndpoint);
    }
    return IcebergCatalogServiceClient.create(settingsBuilder.build());
  }

  private static MetastoreServiceClient createMetastoreServiceClient() throws Exception {
    String customEndpoint = System.getenv("BIGLAKE_API_ENDPOINT");
    MetastoreServiceSettings.Builder settingsBuilder = MetastoreServiceSettings.newBuilder();
    if (customEndpoint != null && !customEndpoint.trim().isEmpty()) {
      settingsBuilder.setEndpoint(customEndpoint);
    }
    return MetastoreServiceClient.create(settingsBuilder.build());
  }
}
