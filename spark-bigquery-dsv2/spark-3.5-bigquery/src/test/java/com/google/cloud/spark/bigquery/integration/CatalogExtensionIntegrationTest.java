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

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class CatalogExtensionIntegrationTest extends CatalogIntegrationTestBase {

  @Test
  public void testCreateGlobalTemporaryView() throws Exception {
    String dataset = testDataset.testDataset;
    String view = "test_view_" + System.nanoTime();

    try (SparkSession spark = createSparkSession()) {
      // prepare the table
      spark.sql("CREATE TABLE " + fullTableName(dataset) + " AS SELECT 1 AS id, 'foo' AS data;");
      Table table = bigquery.getTable(TableId.of(dataset, testTable));
      assertThat(table).isNotNull();
      assertThat(selectCountStarFrom(dataset, testTable)).isEqualTo(1L);

      spark.sql(
          String.format(
              "CREATE GLOBAL TEMPORARY VIEW %s AS select * from %s", view, fullTableName(dataset)));
      List<Row> result = spark.sql("SELECT * FROM global_temp." + view).collectAsList();
      assertThat(result).hasSize(1);
      spark.catalog().listTables("global_temp").show();
    }
  }

  @Test
  public void foo() throws Exception {
    String dataset = testDataset.testDataset;
    String view = "test_view_" + System.nanoTime();
    try (SparkSession spark =
        SparkSession.builder()
            .appName("catalog extension test")
            .master("local[*]")
            .config("spark.sql.legacy.createHiveTableByDefault", "false")
            .config("spark.sql.sources.default", "bigquery")
            .config("spark.datasource.bigquery.writeMethod", "direct")
            .getOrCreate(); ) {
      spark.read().format("bigquery").load("davidrab.demo").createGlobalTempView("vdemo");
      List<Row> result = spark.sql("SELECT * FROM global_temp.vdemo").collectAsList();
      assertThat(result).hasSize(2);
      spark.catalog().listTables("global_temp").show();
    }
  }

  @Override
  protected SparkSession createSparkSession() {
    return SparkSession.builder()
        .appName("catalog extension test")
        .master("local[*]")
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
        .config("spark.sql.sources.default", "bigquery")
        .config("spark.datasource.bigquery.writeMethod", "direct")
        .config(
            "spark.sql.catalog.spark_catalog",
            "com.google.cloud.spark.bigquery.BigQueryCatalogExtension")
        .getOrCreate();
  }
}
