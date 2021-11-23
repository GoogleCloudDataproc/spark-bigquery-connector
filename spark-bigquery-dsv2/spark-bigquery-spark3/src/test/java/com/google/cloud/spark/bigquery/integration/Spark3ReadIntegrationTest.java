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

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class Spark3ReadIntegrationTest extends ReadIntegrationTestBase {
  @Test
  public void testKnownSizeInBytes() {
    Dataset<Row> allTypesTable = readAllTypesTable();
    long actualTableSize =
        allTypesTable.queryExecution().analyzed().stats().sizeInBytes().longValue();
    assertThat(actualTableSize).isEqualTo(TestConstants.ALL_TYPES_TABLE_SIZE);
  }

  @Test
  public void testArrowCompressionCodec() {
    List<Row> avroResults =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("filter", "word_count = 1 OR corpus_date = 0")
            .option("readDataFormat", "AVRO")
            .load()
            .collectAsList();

    List<Row> arrowResultsForZstdCodec =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "ARROW")
            .option("arrowCompressionCodec", "ZSTD")
            .load()
            .where("word_count = 1 OR corpus_date = 0")
            .collectAsList();

    assertThat(avroResults).isEqualTo(arrowResultsForZstdCodec);

    List<Row> arrowResultsForLZ4FrameCodec =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "ARROW")
            .option("arrowCompressionCodec", "LZ4_FRAME")
            .load()
            .where("word_count = 1 OR corpus_date = 0")
            .collectAsList();

    assertThat(avroResults).isEqualTo(arrowResultsForLZ4FrameCodec);
  }
}
