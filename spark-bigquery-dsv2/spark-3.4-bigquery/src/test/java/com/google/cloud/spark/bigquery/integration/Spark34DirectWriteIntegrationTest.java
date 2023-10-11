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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class Spark34DirectWriteIntegrationTest extends WriteIntegrationTestBase {

  public Spark34DirectWriteIntegrationTest() {
    super(SparkBigQueryConfig.WriteMethod.DIRECT, DataTypes.TimestampNTZType);
  }

  // tests from superclass
  @Test
  public void testTimestampNTZWriteToBigQuery() throws InterruptedException {
    LocalDateTime time = LocalDateTime.of(2023, 9, 1, 12, 23, 34, 268543 * 1000);
    List<Row> rows = Arrays.asList(RowFactory.create(time));
    Dataset<Row> df =
        spark.createDataFrame(
            rows,
            new StructType(
                new StructField[] {
                  StructField.apply("foo", DataTypes.TimestampNTZType, true, Metadata.empty())
                }));
    String table = testDataset.toString() + "." + testTable;
    df.write()
        .format("bigquery")
        .mode(SaveMode.Overwrite)
        .option("table", table)
        .option("writeMethod", SparkBigQueryConfig.WriteMethod.DIRECT.toString())
        .save();
    BigQuery bigQuery = IntegrationTestUtils.getBigquery();
    TableResult result =
        bigQuery.query(
            QueryJobConfiguration.of(String.format("Select foo from %s", fullTableName())));
    assertThat(result.getSchema().getFields().get(0).getType())
        .isEqualTo(LegacySQLTypeName.DATETIME);
    assertThat(result.streamValues().findFirst().get().get(0).getValue())
        .isEqualTo("2023-09-01T12:23:34.268543");
  }
}
