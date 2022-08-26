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
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeThat;

import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class DataSourceV1WriteIntegrationTestBase extends WriteIntegrationTestBase {

  public DataSourceV1WriteIntegrationTestBase(SparkBigQueryConfig.WriteMethod writeMethod) {
    super(writeMethod);
  }

  // DSv2 does not support BigNumeric yet
  @Test
  public void testWriteAllDataTypes() {

    // temporarily skipping for v1, as "AVRO" write format is throwing error
    // while writing to GCS
    Dataset<Row> allTypesTable = readAllTypesTable();
    writeToBigQuery(allTypesTable, SaveMode.Overwrite, "avro");

    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset.toString())
            .option("table", testTable)
            .option("readDataFormat", "arrow")
            .load()
            .cache();

    IntegrationTestUtils.compareRows(df.head(), allTypesTable.head());

    // read from cache
    IntegrationTestUtils.compareRows(df.head(), allTypesTable.head());
    IntegrationTestUtils.compareBigNumericDataSetSchema(df.schema(), allTypesTable.schema());
  }

  // v2 does not support ORC
  @Test
  public void testWriteToBigQuery_OrcFormat() throws InterruptedException {
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.INDIRECT));
    // required by ORC
    spark.conf().set("spark.sql.orc.impl", "native");
    writeToBigQuery(initialData(), SaveMode.ErrorIfExists, "orc");
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  // v2 does not support parquet
  @Test
  public void testWriteToBigQuery_ParquetFormat() throws InterruptedException {
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.INDIRECT));
    writeToBigQuery(initialData(), SaveMode.ErrorIfExists, "parquet");
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQuery_UnsupportedFormat() {
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.INDIRECT));
    assertThrows(
        Exception.class,
        () -> {
          writeToBigQuery(initialData(), SaveMode.ErrorIfExists, "something else");
        });
  }

  @Test(timeout = 120_000)
  public void testStreamingToBigQueryWriteAppend() throws Exception {
    StructType schema = initialData().schema();
    ExpressionEncoder<Row> expressionEncoder = RowEncoder.apply(schema);
    MemoryStream<Row> stream = MemoryStream.apply(expressionEncoder, spark.sqlContext());
    long lastBatchId = 0;
    Dataset<Row> streamingDF = stream.toDF();
    String cpLoc = String.format("/tmp/%s-%d", fullTableName(), System.nanoTime());
    // Start write stream
    StreamingQuery writeStream =
        streamingDF
            .writeStream()
            .format("bigquery")
            .outputMode(OutputMode.Append())
            .option("checkpointLocation", cpLoc)
            .option("table", fullTableName())
            .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
            .start();

    // Write to stream
    stream.addData(toSeq(initialData().collectAsList()));
    while (writeStream.lastProgress().batchId() <= lastBatchId) {
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
    lastBatchId = writeStream.lastProgress().batchId();
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    // Write to stream
    stream.addData(toSeq(additonalData().collectAsList()));
    while (writeStream.lastProgress().batchId() <= lastBatchId) {
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
    writeStream.stop();
    assertThat(testTableNumberOfRows()).isEqualTo(4);
    assertThat(additionalDataValuesExist()).isTrue();
  }

  private static <T> Seq<T> toSeq(List<T> list) {
    return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
  }

  // additional tests are from the super-class

}
