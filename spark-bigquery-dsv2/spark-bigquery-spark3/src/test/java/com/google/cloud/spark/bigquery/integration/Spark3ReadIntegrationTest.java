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

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class Spark3ReadIntegrationTest extends ReadIntegrationTestBase {
  private static final String LARGE_TABLE = "bigquery-public-data.samples.natality";
  private static final String LARGE_TABLE_FIELD = "is_male";
  private static final long LARGE_TABLE_NUM_ROWS = 33271914L;
  private static final String NON_EXISTENT_TABLE = "non-existent.non-existent.non-existent";
  private static final String STRUCT_COLUMN_ORDER_TEST_TABLE_NAME = "struct_column_order";
  private static final String ALL_TYPES_TABLE_NAME = "all_types";
  private static final String ALL_TYPES_VIEW_NAME = "all_types_view";
  // tests are from the super-class
  @Test
  public void testCountWithFilters() {
    long countResults =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "ARROW")
            .load()
            .where("word_count = 1 OR corpus_date = 0")
            .count();

    long countAfterCollect =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "ARROW")
            .load()
            .where("word_count = 1 OR corpus_date = 0")
            .collectAsList()
            .size();

    assertThat(countResults).isEqualTo(countAfterCollect);
  }

  @Test
  public void testKnownSizeInBytes() {
    Dataset<Row> allTypesTable = readAllTypesTable();
    long actualTableSize =
        allTypesTable.queryExecution().analyzed().stats().sizeInBytes().longValue();
    assertThat(actualTableSize).isEqualTo(TestConstants.ALL_TYPES_TABLE_SIZE);
  }

  @Test
  public void testKnownSchema() {
    Dataset<Row> allTypesTable = readAllTypesTable();
    assertThat(allTypesTable.schema()).isEqualTo(TestConstants.ALL_TYPES_TABLE_SCHEMA);
  }

  @Test
  public void testUserDefinedSchema() {
    // TODO(pmkc): consider a schema that wouldn't cause cast errors if read.
    StructType expectedSchema =
        StructType.fromDDL("`word` STRING COMMENT 'A single unique word (where whitespace is the delimiter) extracted from a corpus.',`word_count` BIGINT COMMENT 'The number of times this word appears in this corpus.',`corpus` STRING COMMENT 'The work from which this word was extracted.',`corpus_date` BIGINT COMMENT 'The year in which this corpus was published.'");
    Dataset<Row> table =
        spark
            .read()
            .schema(expectedSchema)
            .format("bigquery")
            .option("table", TestConstants.SHAKESPEARE_TABLE)
            .load();
    assertThat(expectedSchema).isEqualTo(StructType.fromDDL(table.schema().toDDL()));
  }

  @Test
  public void testNonExistentSchema() {
    assertThrows(
        "Trying to read a non existing table should throw an exception",
        RuntimeException.class,
        () -> {
          spark.read().format("bigquery").option("table", NON_EXISTENT_TABLE).load();
        });
  }

  @Test(timeout = 40_000) // 10 seconds
  public void testHeadDoesNotTimeoutAndOOM() {
    spark
        .read()
        .format("bigquery")
        .option("table", LARGE_TABLE)
        .load()
        .select(LARGE_TABLE_FIELD)
        .head();
  }

  @Test
  public void testUnhandleFilterOnStruct() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data:samples.github_nested")
            .option("filter", "url like '%spark'")
            .load();

    List<Row> result = df.select("url").where("repository is not null").collectAsList();

    assertThat(result).hasSize(88);
  }

  @Test
  public void testQueryMaterializedView() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data:ethereum_blockchain.live_logs")
            .option("viewsEnabled", "true")
            .option("viewMaterializationProject", PROJECT_ID)
            .option("viewMaterializationDataset", testDataset.toString())
            .load();

    assertThat(df.count()).isGreaterThan(1);
  }

  @Test
  public void testOrAcrossColumnsAndFormats() {
    List<Row> avroResults =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("filter", "word_count = 1 OR corpus_date = 0")
            .option("readDataFormat", "AVRO")
            .load()
            .collectAsList();
    List<Row> arrowResults =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "ARROW")
                .option("filter", "word_count = 1 OR corpus_date = 0")
            .load()
            .collectAsList();
    assertThat(avroResults).isEqualTo(arrowResults);
  }
}
