/*
 * Copyright 2020 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.write.BigQueryWriteHelper;
import com.google.common.collect.Streams;
import java.util.Optional;
import java.util.ServiceLoader;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BigQueryStreamWriter {

  private static final Logger log = LoggerFactory.getLogger(BigQueryStreamWriter.class);

  private BigQueryStreamWriter() {}

  /**
   * Convert streaming dataframe to fixed dataframe by Getting partitioned RDD and mapping to Row
   * dataframe
   *
   * @param data Streaming dataframe
   * @param sqlContext Spark SQLContext
   * @param opts Spark BigQuery Options
   */
  public static void writeBatch(
      Dataset<Row> data,
      SQLContext sqlContext,
      OutputMode outputMode,
      SparkBigQueryConfig opts,
      BigQueryClient bigQueryClient) {
    StructType schema = data.schema();
    String sparkVersion = sqlContext.sparkSession().version();

    // Assuming DataFrameToRDDConverter.convertToRDD returns RDD<Row>
    // The original Scala code uses a polymorphic dispatch.
    // In Java, this might involve an explicit cast or a generic type.
    RDD<Row> rowRdd = dataFrameToRDDConverterFactory(sparkVersion).convertToRDD(data);

    // Create fixed dataframe
    Dataset<Row> dataFrame = sqlContext.createDataFrame(rowRdd, schema);
    TableInfo table =
        bigQueryClient.getTable(opts.getTableId()); // returns Table, not Optional<Table>
    SaveMode saveMode = getSaveMode(outputMode);

    BigQueryWriteHelper helper =
        new BigQueryWriteHelper(
            bigQueryClient, sqlContext, saveMode, opts, dataFrame, Optional.ofNullable(table));
    helper.writeDataFrameToBigQuery();
  }

  /**
   * Convert Output mode to save mode Complete => Truncate Append => Append (Default) Update => Not
   * yet supported
   *
   * @param outputMode
   * @return SaveMode
   * @throws UnsupportedOperationException
   */
  private static SaveMode getSaveMode(OutputMode outputMode) {
    if (outputMode.equals(OutputMode.Complete())) { // Use .equals() for object comparison
      return SaveMode.Overwrite;
    } else if (outputMode.equals(OutputMode.Update())) {
      throw new UnsupportedOperationException("Updates are not yet supported");
    } else { // Append or other modes
      return SaveMode.Append;
    }
  }

  public static DataFrameToRDDConverter dataFrameToRDDConverterFactory(String sparkVersion) {
    ServiceLoader<DataFrameToRDDConverter> serviceLoader =
        ServiceLoader.load(DataFrameToRDDConverter.class);
    return Streams.stream(serviceLoader.iterator())
        .filter(converter -> converter.supports(sparkVersion))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Could not find an implementation of "
                        + DataFrameToRDDConverter.class.getCanonicalName()));
  }
}
