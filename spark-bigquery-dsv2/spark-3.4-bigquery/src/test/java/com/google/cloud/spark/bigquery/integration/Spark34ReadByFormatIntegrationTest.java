/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.*;
import java.time.LocalDateTime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class Spark34ReadByFormatIntegrationTest extends ReadByFormatIntegrationTestBase {

  public Spark34ReadByFormatIntegrationTest() {
    super("ARROW", /* userProvidedSchemaAllowed */ false);
  }

  // tests are from the super-class
  @Test
  public void testTimestampNTZReadFromBigQuery() {
    BigQuery bigQuery = IntegrationTestUtils.getBigquery();
    LocalDateTime dateTime = LocalDateTime.of(2023, 9, 18, 14, 30, 15, 234 * 1_000_000);
    bigQuery.create(
        TableInfo.newBuilder(
                TableId.of(testDataset.toString(), testTable),
                StandardTableDefinition.of(Schema.of(Field.of("foo", LegacySQLTypeName.DATETIME))))
            .build());
    IntegrationTestUtils.runQuery(
        String.format(
            "INSERT INTO %s.%s (foo) VALUES " + "('%s')", testDataset, testTable, dateTime));

    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset.toString())
            .option("table", testTable)
            .load();
    StructType schema = df.schema();
    assertThat(schema.apply("foo").dataType()).isEqualTo(DataTypes.TimestampNTZType);
    Row row = df.head();
    assertThat(row.get(0)).isEqualTo(dateTime);
  }
}
