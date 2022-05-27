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
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spark.bigquery.integration.model.ColumnOrderTestClass;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class ReadByFormatIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

  protected final String dataFormat;
  protected final boolean userProvidedSchemaAllowed;

  public ReadByFormatIntegrationTestBase(String dataFormat) {
    this(dataFormat, true);
  }

  public ReadByFormatIntegrationTestBase(String dataFormat, boolean userProvidedSchemaAllowed) {
    super();
    this.dataFormat = dataFormat;
    this.userProvidedSchemaAllowed = userProvidedSchemaAllowed;
  }

  @Test
  public void testViewWithDifferentColumnsForSelectAndFilter() {

    Dataset<Row> df = getViewDataFrame();

    // filer and select are pushed down to BQ
    List<Row> result = df.select("int_req").filter("str = 'string'").collectAsList();

    assertThat(result).hasSize(1);
    List<Row> filteredResult =
        result.stream().filter(row -> row.getLong(0) == 42L).collect(Collectors.toList());
    assertThat(filteredResult).hasSize(1);
  }

  @Test
  public void testCachedViewWithDifferentColumnsForSelectAndFilter() {

    Dataset<Row> df = getViewDataFrame();
    Dataset<Row> cachedDF = df.cache();

    // filter and select are run on the spark side as the view was cached
    List<Row> result = cachedDF.select("int_req").filter("str = 'string'").collectAsList();

    assertThat(result).hasSize(1);
    List<Row> filteredResult =
        result.stream().filter(row -> row.getLong(0) == 42L).collect(Collectors.toList());
    assertThat(filteredResult).hasSize(1);
  }

  @Test
  public void testOutOfOrderColumns() {
    Row row =
        spark
            .read()
            .format("bigquery")
            .option("table", TestConstants.SHAKESPEARE_TABLE)
            .option("readDataFormat", dataFormat)
            .load()
            .select("word_count", "word")
            .head();
    assertThat(row.get(0)).isInstanceOf(Long.class);
    assertThat(row.get(1)).isInstanceOf(String.class);
  }

  @Test
  public void testSelectAllColumnsFromATable() {
    Row row =
        spark
            .read()
            .format("bigquery")
            .option("table", TestConstants.SHAKESPEARE_TABLE)
            .option("readDataFormat", dataFormat)
            .load()
            .select("word_count", "word", "corpus", "corpus_date")
            .head();
    assertThat(row.get(0)).isInstanceOf(Long.class);
    assertThat(row.get(1)).isInstanceOf(String.class);
    assertThat(row.get(2)).isInstanceOf(String.class);
    assertThat(row.get(3)).isInstanceOf(Long.class);
  }

  @Test
  public void testNumberOfPartitions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", TestConstants.LARGE_TABLE)
            .option("parallelism", "5")
            .option("readDataFormat", dataFormat)
            .load();
    assertThat(df.rdd().getNumPartitions()).isEqualTo(5);
  }

  @Test
  public void testDefaultNumberOfPartitions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", TestConstants.LARGE_TABLE)
            .option("readDataFormat", dataFormat)
            .load();

    assertThat(df.rdd().getNumPartitions()).isEqualTo(58);
  }

  @Test(timeout = 300_000)
  public void testBalancedPartitions() {
    // Select first partition
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("parallelism", 5)
            .option("readDataFormat", dataFormat)
            .option("filter", "year > 2000")
            .load(TestConstants.LARGE_TABLE)
            .select(TestConstants.LARGE_TABLE_FIELD); // minimize payload
    long sizeOfFirstPartition =
        df.rdd()
            .toJavaRDD()
            .mapPartitions(rows -> Arrays.asList(Iterators.size(rows)).iterator())
            .collect()
            .get(0)
            .longValue();

    // Since we are only reading from a single stream, we can expect to get
    // at least as many rows
    // in that stream as a perfectly uniform distribution would command.
    // Note that the assertion
    // is on a range of rows because rows are assigned to streams on the
    // server-side in
    // indivisible units of many rows.

    long numRowsLowerBound = TestConstants.LARGE_TABLE_NUM_ROWS / df.rdd().getNumPartitions();
    assertThat(numRowsLowerBound <= sizeOfFirstPartition).isTrue();
    assertThat(sizeOfFirstPartition < numRowsLowerBound * 1.1).isTrue();
  }

  @Test
  public void testKeepingFiltersBehaviour() {
    Set<String> newBehaviourWords =
        extractWords(
            spark
                .read()
                .format("bigquery")
                .option("table", "bigquery-public-data.samples.shakespeare")
                .option("filter", "length(word) = 1")
                .option("combinePushedDownFilters", "true")
                .option("readDataFormat", dataFormat)
                .load());

    Set<String> oldBehaviourWords =
        extractWords(
            spark
                .read()
                .format("bigquery")
                .option("table", "bigquery-public-data.samples.shakespeare")
                .option("filter", "length(word) = 1")
                .option("combinePushedDownFilters", "false")
                .option("readDataFormat", dataFormat)
                .load());

    assertThat(newBehaviourWords).isEqualTo(oldBehaviourWords);
  }

  @Test
  public void testColumnOrderOfStruct() {
    assumeTrue("user provided schema is no allowed for this connector", userProvidedSchemaAllowed);
    StructType schema = Encoders.bean(ColumnOrderTestClass.class).schema();

    Dataset<ColumnOrderTestClass> dataset =
        spark
            .read()
            .schema(schema)
            .option("dataset", testDataset.toString())
            .option("table", TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_NAME)
            .format("bigquery")
            .option("readDataFormat", dataFormat)
            .load()
            .as(Encoders.bean(ColumnOrderTestClass.class));

    ColumnOrderTestClass row = dataset.head();
    assertThat(row).isEqualTo(TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_COLS);
  }

  Dataset<Row> getViewDataFrame() {
    return spark
        .read()
        .format("bigquery")
        .option("table", TestConstants.ALL_TYPES_VIEW_NAME)
        .option("viewsEnabled", "true")
        .option("viewMaterializationProject", System.getenv("GOOGLE_CLOUD_PROJECT"))
        .option("viewMaterializationDataset", testDataset.toString())
        .option("readDataFormat", dataFormat)
        .load();
  }

  Dataset<Row> readAllTypesTable() {
    return spark
        .read()
        .format("bigquery")
        .option("dataset", testDataset.toString())
        .option("table", TestConstants.ALL_TYPES_TABLE_NAME)
        .load();
  }

  protected Set<String> extractWords(Dataset<Row> df) {
    return ImmutableSet.copyOf(
        df.select("word").where("corpus_date = 0").as(Encoders.STRING()).collectAsList());
  }
}
