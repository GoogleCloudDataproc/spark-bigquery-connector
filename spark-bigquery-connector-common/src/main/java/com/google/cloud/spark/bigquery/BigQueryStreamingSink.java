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

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.streaming.OutputMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Note: Scala case classes have auto-generated equals, hashCode, toString, and constructor.
// For Java, you'd typically use a regular class and implement these if needed, or use records (Java
// 14+).
public class BigQueryStreamingSink implements Sink {

  private static final Logger log = LoggerFactory.getLogger(BigQueryStreamingSink.class);

  private final SQLContext sqlContext;
  private final Map<String, String> parameters;
  private final List<String> partitionColumns; // Changed from Seq<String>
  private final OutputMode outputMode;
  private final SparkBigQueryConfig opts;
  private final BigQueryClient bigQueryClient;

  private volatile long latestBatchId = -1L;

  public BigQueryStreamingSink(
      SQLContext sqlContext,
      Map<String, String> parameters,
      List<String> partitionColumns, // Changed from Seq<String>
      OutputMode outputMode,
      SparkBigQueryConfig opts,
      BigQueryClient bigQueryClient) {
    this.sqlContext = sqlContext;
    this.parameters = parameters;
    this.partitionColumns = partitionColumns;
    this.outputMode = outputMode;
    this.opts = opts;
    this.bigQueryClient = bigQueryClient;
  }

  // The original Scala addBatch is not explicitly throwing IOException,
  // but Sink.addBatch might allow it.
  // In Java, if an overridden method in an interface doesn't declare an exception,
  // the overriding method cannot declare a checked exception.
  // Sink.addBatch in Spark does not declare IOException.
  @Override
  public void addBatch(long batchId, Dataset<Row> data) {
    if (batchId <= latestBatchId) {
      log.warn("Skipping as already committed batch " + batchId);
    } else {
      log.debug("addBatch(" + batchId + ")");
      // Assuming BigQueryStreamWriter.writeBatch doesn't throw checked exceptions
      // or they are handled internally/converted to RuntimeException
      BigQueryStreamWriter.writeBatch(data, sqlContext, outputMode, opts, bigQueryClient);
      latestBatchId = batchId;
    }
  }

  // Getters if needed for the fields (like in Scala case classes)
  public SQLContext getSqlContext() {
    return sqlContext;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public OutputMode getOutputMode() {
    return outputMode;
  }

  public SparkBigQueryConfig getOpts() {
    return opts;
  }

  public BigQueryClient getBigQueryClient() {
    return bigQueryClient;
  }

  public long getLatestBatchId() {
    return latestBatchId;
  }

  // If this class were to be used in contexts requiring equals/hashCode (e.g., collections):
  @Override
  public boolean equals(Object o) {
    // Basic implementation, needs to be more robust for a real case class equivalent
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BigQueryStreamingSink that = (BigQueryStreamingSink) o;
    return latestBatchId == that.latestBatchId
        && java.util.Objects.equals(sqlContext, that.sqlContext)
        && java.util.Objects.equals(parameters, that.parameters)
        && java.util.Objects.equals(partitionColumns, that.partitionColumns)
        && java.util.Objects.equals(outputMode, that.outputMode)
        && java.util.Objects.equals(opts, that.opts)
        && java.util.Objects.equals(bigQueryClient, that.bigQueryClient);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(
        sqlContext, parameters, partitionColumns, outputMode, opts, bigQueryClient, latestBatchId);
  }

  @Override
  public String toString() {
    return "BigQueryStreamingSink("
        + "sqlContext="
        + sqlContext
        + ", parameters="
        + parameters
        + ", partitionColumns="
        + partitionColumns
        + ", outputMode="
        + outputMode
        + ", opts="
        + opts
        + ", bigQueryClient="
        + bigQueryClient
        + ", latestBatchId="
        + latestBatchId
        + ')';
  }
}
