/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class DataSourceV1DirectWriteIntegrationTest extends DataSourceV1WriteIntegrationTestBase {

  public DataSourceV1DirectWriteIntegrationTest() {
    super(SparkBigQueryConfig.WriteMethod.DIRECT);
  }

  // additional tests are from the super-class
  @Test
  public void testPartitionBy() throws Exception {
    Dataset<Row> df =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(java.sql.Date.valueOf("2020-01-01"), 1),
                RowFactory.create(java.sql.Date.valueOf("2020-01-02"), 2),
                RowFactory.create(java.sql.Date.valueOf("2020-01-03"), 3)),
            new StructType(
                new StructField[] {
                    StructField.apply("d", DataTypes.DateType, true, Metadata.empty()),
                    StructField.apply("n", DataTypes.IntegerType, true, Metadata.empty())
                }));

    df.write()
        .format("bigquery")
        .option("table", fullTableName())
        .option("writeMethod", writeMethod.toString())
        .partitionBy("d")
        .save();
    Dataset<Row> resultDF = spark.read().format("bigquery").load(fullTableName());
    assertThat(resultDF.count()).isEqualTo(3);
  }
}
