package com.google.cloud.spark.bigquery.integration;

import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class CatalogIntegrationTest {

  @ClassRule
  public static TestDataset testDataset = new TestDataset();

  private String testTable;

  @Before
  public void renameTestTable() {
    testTable = String.format("test_%s_%s", Long.toHexString(System.currentTimeMillis()), Long.toHexString(System.nanoTime()));
  }

  @Test
  public void testCatalogUsage() throws Exception {
    SparkSession spark =
        SparkSession.builder()
            .appName("catalog test")
            .master("local[*]")
            .config("spark.sql.legacy.createHiveTableByDefault", "false")
            .config("spark.sql.sources.default", "bigquery")
            .config("spark.datasource.bigquery.writeMethod", "direct")
            .config("spark.sql.defaultCatalog", "bigquery")
            .config(
                "spark.sql.catalog.bigquery",
                "com.google.cloud.spark.bigquery.v2.BigQueryTableCatalog")
            .getOrCreate();

    spark.sql("CREATE TABLE " + testTable + "(id int, data string);").show();
  }
}
