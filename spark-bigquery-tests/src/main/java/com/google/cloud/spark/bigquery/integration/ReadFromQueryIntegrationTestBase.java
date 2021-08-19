/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Test;

class ReadFromQueryIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

  private static final String  ALL_TYPES_TABLE_NAME = "all_types";
  private BigQuery bq;

  protected ReadFromQueryIntegrationTestBase(IntegrationTestContext ctx) {
    super(ctx);
    this.bq = BigQueryOptions.getDefaultInstance().getService();
  }

  private void testReadFromQueryInternal(String query) {
    Dataset<Row> df = spark.read().format("bigquery")
      .option("viewsEnabled", true)
      .option("materializationDataset", testDataset)
      .load(query);

  validateResult(df);
  }

  @Test
  public void testReadFromQuery() {
    // the query suffix is to make sure that each format will have
    // a different table created due to the destination table cache
    String random = String.valueOf(System.nanoTime());
    String query = String.format(
        "SELECT corpus, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE word='spark' AND '%s'='%s'",
        random, random);
    testReadFromQueryInternal(query);
  }

  @Test
  public void testReadFromQueryWithNewLine() {
    // the query suffix is to make sure that each format will have
    // a different table created due to the destination table cache
    String random = String.valueOf(System.nanoTime());
    String query = String.format(
        "SELECT corpus, word_count FROM `bigquery-public-data.samples.shakespeare`\n"+
            "WHERE word='spark' AND '%s'='%s'",
        random, random);
    testReadFromQueryInternal(query);
  }

  @Test
  public void testQueryOption() {
    // the query suffix is to make sure that each format will have
    // a different table created due to the destination table cache
    String random = String.valueOf(System.nanoTime());
    String query = String.format(
        "SELECT corpus, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE word='spark' AND '%s'='%s'",
        random, random);
    Dataset<Row> df = spark.read().format("bigquery")
        .option("viewsEnabled", true)
        .option("materializationDataset", testDataset)
        .option("query", query)
        .load();

    validateResult(df);
  }

  private void validateResult(Dataset<Row> df) {
    long totalRows = df.count();
    assertThat(totalRows).isEqualTo(9);

    List<String> corpuses = df.select("corpus").as(Encoders.STRING()).collectAsList();
    List<String> expectedCorpuses = Arrays
        .asList("2kinghenryvi", "3kinghenryvi", "allswellthatendswell", "hamlet",
            "juliuscaesar", "kinghenryv", "kinglear", "periclesprinceoftyre", "troilusandcressida");
    assertThat(corpuses).containsExactlyElementsIn(expectedCorpuses);
  }

  @Test public void
  testBadQuery() {
    String badSql = "SELECT bogus_column FROM `bigquery-public-data.samples.shakespeare`";
    // v1 throws BigQueryConnectorException
    // v2 throws Guice ProviderException, as the table is materialized in teh module
    assertThrows(RuntimeException.class, () -> {
      spark.read().format("bigquery")
        .option("viewsEnabled", true)
        .option("materializationDataset", testDataset)
        .load(badSql);
    });
  }

}

