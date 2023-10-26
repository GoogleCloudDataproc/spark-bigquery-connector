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
import org.junit.Before;
import org.junit.Test;

public class Spark34IndirectWriteIntegrationTest extends WriteIntegrationTestBase {

  public Spark34IndirectWriteIntegrationTest() {
    super(SparkBigQueryConfig.WriteMethod.DIRECT, DataTypes.TimestampNTZType);
  }

  @Before
  public void setParquetLoadBehaviour() {
    // TODO: make this the default value
    spark.conf().set("enableListInference", "true");
  }

  private TableResult insertAndGetTimestampNTZToBigQuery(LocalDateTime time, String format)
      throws InterruptedException {
    List<Row> rows = Arrays.asList(RowFactory.create(time));
    Dataset<Row> df =
        spark.createDataFrame(
            rows,
            new StructType(
                new StructField[] {
                  StructField.apply("foo", DataTypes.TimestampNTZType, true, Metadata.empty())
                }));
    writeToBigQuery(df, SaveMode.Overwrite, format);
    BigQuery bigQuery = IntegrationTestUtils.getBigquery();
    TableResult result =
        bigQuery.query(
            QueryJobConfiguration.of(String.format("Select foo from %s", fullTableName())));
    return result;
  }

  // tests from superclass
  @Test
  public void testTimestampNTZWriteToBigQueryAvroFormat() throws InterruptedException {
    LocalDateTime time = LocalDateTime.of(2023, 9, 1, 12, 23, 34, 268543 * 1000);
    TableResult result = insertAndGetTimestampNTZToBigQuery(time, "avro");
    assertThat(result.getSchema().getFields().get(0).getType())
        .isEqualTo(LegacySQLTypeName.DATETIME);
    assertThat(result.streamValues().findFirst().get().get(0).getValue())
        .isEqualTo("2023-09-01T12:23:34.268543");
  }

  @Test
  public void testTimestampNTZWriteToBigQueryParquetFormat() throws InterruptedException {
    LocalDateTime time = LocalDateTime.of(2023, 9, 15, 12, 36, 34, 268543 * 1000);
    TableResult result = insertAndGetTimestampNTZToBigQuery(time, "parquet");
    assertThat(result.getSchema().getFields().get(0).getType())
        .isEqualTo(LegacySQLTypeName.DATETIME);
    assertThat(result.streamValues().findFirst().get().get(0).getValue())
        .isEqualTo("2023-09-15T12:36:34.268543");
  }
}
