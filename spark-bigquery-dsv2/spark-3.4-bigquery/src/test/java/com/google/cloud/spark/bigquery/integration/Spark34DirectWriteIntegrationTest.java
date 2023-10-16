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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assume.assumeThat;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;
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
    super(SparkBigQueryConfig.WriteMethod.DIRECT);
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

  @Test
  public void testOverwriteDynamicPartition_partitionDateTimeByHour() {
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.DIRECT));
    String orderId = "order_id";
    String orderDateTime = "order_date_time";
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE `%s.%s` (%s INTEGER, %s DATETIME) "
                + "PARTITION BY timestamp_trunc(order_date_time, HOUR) "
                + "AS SELECT * FROM UNNEST([(1, DATETIME '2023-09-28 1:00:00'), "
                + "(2, DATETIME '2023-09-28 10:00:00'), (3, DATETIME '2023-09-28 10:30:00')])",
            testDataset, testTable, orderId, orderDateTime));

    Dataset<Row> df =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(10, LocalDateTime.of(2023, 9, 28, 10, 15, 0)),
                RowFactory.create(20, LocalDateTime.of(2023, 9, 30, 12, 0, 0))),
            structType(
                StructField.apply(orderId, DataTypes.IntegerType, true, Metadata.empty()),
                StructField.apply(
                    orderDateTime, DataTypes.TimestampNTZType, true, Metadata.empty())));

    Dataset<Row> result = writeAndLoadDatasetOverwriteDynamicPartition(df);
    assertThat(result.count()).isEqualTo(3);
    List<Row> rows = result.collectAsList();
    rows.sort(Comparator.comparing(row -> row.getLong(row.fieldIndex(orderId))));

    Row row1 = rows.get(0);
    Row row2 = rows.get(1);
    Row row3 = rows.get(2);

    assertThat(row1.getLong(row1.fieldIndex(orderId))).isEqualTo(1);
    assertThat(row1.get(row1.fieldIndex(orderDateTime)).toString()).isEqualTo("2023-09-28T01:00");

    assertThat(row2.getLong(row2.fieldIndex(orderId))).isEqualTo(10);
    assertThat(row2.get(row2.fieldIndex(orderDateTime)).toString()).isEqualTo("2023-09-28T10:15");

    assertThat(row3.getLong(row3.fieldIndex(orderId))).isEqualTo(20);
    assertThat(row3.get(row3.fieldIndex(orderDateTime)).toString()).isEqualTo("2023-09-30T12:00");
  }

  @Test
  public void testOverwriteDynamicPartition_partitionDateTimeByDay() {
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.DIRECT));
    String orderId = "order_id";
    String orderDateTime = "order_date_time";
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE `%s.%s` (%s INTEGER, %s DATETIME) "
                + "PARTITION BY timestamp_trunc(order_date_time, DAY) "
                + "AS SELECT * FROM UNNEST([(1, DATETIME '2023-09-28 1:00:00'), "
                + "(2, DATETIME '2023-09-29 10:00:00'), (3, DATETIME '2023-09-29 17:30:00')])",
            testDataset, testTable, orderId, orderDateTime));

    Dataset<Row> df =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(10, LocalDateTime.of(2023, 9, 29, 10, 15, 0)),
                RowFactory.create(20, LocalDateTime.of(2023, 9, 30, 12, 0, 0))),
            structType(
                StructField.apply(orderId, DataTypes.IntegerType, true, Metadata.empty()),
                StructField.apply(
                    orderDateTime, DataTypes.TimestampNTZType, true, Metadata.empty())));

    Dataset<Row> result = writeAndLoadDatasetOverwriteDynamicPartition(df);
    assertThat(result.count()).isEqualTo(3);
    List<Row> rows = result.collectAsList();
    rows.sort(Comparator.comparing(row -> row.getLong(row.fieldIndex(orderId))));

    Row row1 = rows.get(0);
    Row row2 = rows.get(1);
    Row row3 = rows.get(2);

    assertThat(row1.getLong(row1.fieldIndex(orderId))).isEqualTo(1);
    assertThat(row1.get(row1.fieldIndex(orderDateTime)).toString()).isEqualTo("2023-09-28T01:00");

    assertThat(row2.getLong(row2.fieldIndex(orderId))).isEqualTo(10);
    assertThat(row2.get(row2.fieldIndex(orderDateTime)).toString()).isEqualTo("2023-09-29T10:15");

    assertThat(row3.getLong(row3.fieldIndex(orderId))).isEqualTo(20);
    assertThat(row3.get(row3.fieldIndex(orderDateTime)).toString()).isEqualTo("2023-09-30T12:00");
  }

  @Test
  public void testOverwriteDynamicPartition_partitionDateTimeByMonth() {
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.DIRECT));
    String orderId = "order_id";
    String orderDateTime = "order_date_time";
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE `%s.%s` (%s INTEGER, %s DATETIME) "
                + "PARTITION BY timestamp_trunc(order_date_time, MONTH) "
                + "AS SELECT * FROM UNNEST([(1, DATETIME '2023-09-28 1:00:00'), "
                + "(2, DATETIME '2023-10-29 10:00:00'), (3, DATETIME '2023-10-29 17:30:00')])",
            testDataset, testTable, orderId, orderDateTime));

    Dataset<Row> df =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(10, LocalDateTime.of(2023, 10, 20, 10, 15, 0)),
                RowFactory.create(20, LocalDateTime.of(2023, 11, 30, 12, 0, 0))),
            structType(
                StructField.apply(orderId, DataTypes.IntegerType, true, Metadata.empty()),
                StructField.apply(
                    orderDateTime, DataTypes.TimestampNTZType, true, Metadata.empty())));

    Dataset<Row> result = writeAndLoadDatasetOverwriteDynamicPartition(df);
    assertThat(result.count()).isEqualTo(3);
    List<Row> rows = result.collectAsList();
    rows.sort(Comparator.comparing(row -> row.getLong(row.fieldIndex(orderId))));

    Row row1 = rows.get(0);
    Row row2 = rows.get(1);
    Row row3 = rows.get(2);

    assertThat(row1.getLong(row1.fieldIndex(orderId))).isEqualTo(1);
    assertThat(row1.get(row1.fieldIndex(orderDateTime)).toString()).isEqualTo("2023-09-28T01:00");

    assertThat(row2.getLong(row2.fieldIndex(orderId))).isEqualTo(10);
    assertThat(row2.get(row2.fieldIndex(orderDateTime)).toString()).isEqualTo("2023-10-20T10:15");

    assertThat(row3.getLong(row3.fieldIndex(orderId))).isEqualTo(20);
    assertThat(row3.get(row3.fieldIndex(orderDateTime)).toString()).isEqualTo("2023-11-30T12:00");
  }

  @Test
  public void testOverwriteDynamicPartition_partitionDateTimeByYear() {
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.DIRECT));
    String orderId = "order_id";
    String orderDateTime = "order_date_time";
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE `%s.%s` (%s INTEGER, %s DATETIME) "
                + "PARTITION BY timestamp_trunc(order_date_time, YEAR) "
                + "AS SELECT * FROM UNNEST([(1, DATETIME '2022-09-28 1:00:00'), "
                + "(2, DATETIME '2023-10-29 10:00:00'), (3, DATETIME '2023-11-29 17:30:00')])",
            testDataset, testTable, orderId, orderDateTime));

    Dataset<Row> df =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(10, LocalDateTime.of(2023, 10, 20, 10, 15, 0)),
                RowFactory.create(20, LocalDateTime.of(2024, 11, 30, 12, 0, 0))),
            structType(
                StructField.apply(orderId, DataTypes.IntegerType, true, Metadata.empty()),
                StructField.apply(
                    orderDateTime, DataTypes.TimestampNTZType, true, Metadata.empty())));

    Dataset<Row> result = writeAndLoadDatasetOverwriteDynamicPartition(df);
    assertThat(result.count()).isEqualTo(3);
    List<Row> rows = result.collectAsList();
    rows.sort(Comparator.comparing(row -> row.getLong(row.fieldIndex(orderId))));

    Row row1 = rows.get(0);
    Row row2 = rows.get(1);
    Row row3 = rows.get(2);

    assertThat(row1.getLong(row1.fieldIndex(orderId))).isEqualTo(1);
    assertThat(row1.get(row1.fieldIndex(orderDateTime)).toString()).isEqualTo("2022-09-28T01:00");

    assertThat(row2.getLong(row2.fieldIndex(orderId))).isEqualTo(10);
    assertThat(row2.get(row2.fieldIndex(orderDateTime)).toString()).isEqualTo("2023-10-20T10:15");

    assertThat(row3.getLong(row3.fieldIndex(orderId))).isEqualTo(20);
    assertThat(row3.get(row3.fieldIndex(orderDateTime)).toString()).isEqualTo("2024-11-30T12:00");
  }
}
