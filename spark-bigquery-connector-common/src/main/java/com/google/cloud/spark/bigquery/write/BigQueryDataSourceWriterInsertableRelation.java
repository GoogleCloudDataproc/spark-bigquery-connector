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
package com.google.cloud.spark.bigquery.write;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.write.context.BigQueryDirectDataSourceWriterContext;
import com.google.cloud.spark.bigquery.write.context.BigQueryIndirectDataSourceWriterContext;
import com.google.cloud.spark.bigquery.write.context.DataSourceWriterContext;
import com.google.cloud.spark.bigquery.write.context.WriterCommitMessageContext;
import com.google.inject.Injector;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class BigQueryDataSourceWriterInsertableRelation extends BigQueryInsertableRelationBase {

  private final Injector injector;

  public BigQueryDataSourceWriterInsertableRelation(
      BigQueryClient bigQueryClient,
      SQLContext sqlContext,
      SparkBigQueryConfig config,
      Injector injector) {
    super(bigQueryClient, sqlContext, config);
    this.injector = injector;
  }

  @Override
  public void insert(Dataset<Row> data, boolean overwrite) {
    logger.debug("Inserting data={}, overwrite={}", data, overwrite);
    // Creating the context is deferred to here as the direct DataSourceWriterContext creates the
    // table upon construction, which interferes with teh ErrorIfExists and Ignore save modes.
    DataSourceWriterContext ctx = null;
    SparkBigQueryConfig.WriteMethod writeMethod = config.getWriteMethod();
    if (writeMethod == SparkBigQueryConfig.WriteMethod.DIRECT) {
      ctx = injector.getInstance(BigQueryDirectDataSourceWriterContext.class);
    } else if (writeMethod == SparkBigQueryConfig.WriteMethod.INDIRECT) {
      ctx = injector.getInstance(BigQueryIndirectDataSourceWriterContext.class);
    } else {
      // can't really happen, here to guard from new write methods
      throw new IllegalArgumentException("Unknown write method " + writeMethod);
    }
    // Here we are mimicking the DataSource v2 API behaviour in oder to use the shared code. The
    // partition handler iterates on each partition separately, invoking the DataWriter interface.
    // The result of the iteration is a WriterCommitMessageContext which is used to perform the
    // global commit, or abort if needed.
    try {
      JavaRDD<Row> rowsRDD = data.toJavaRDD();
      int numPartitions = rowsRDD.getNumPartitions();
      if (numPartitions == 0) {
        // The DataFrame is empty, no streams will be generated. We need to create the table if it
        // does not exist.
        Schema bigQuerySchema =
            SchemaConverters.from(SchemaConvertersConfiguration.from(config))
                .toBigQuerySchema(data.schema());
        bigQueryClient.createTableIfNeeded(getTableId(), bigQuerySchema, config);
      } else {
        // Write the data into separate WriteStream (one oer partition, return the
        // WriterCommitMessageContext containing the stream name.
        DataSourceWriterContextPartitionHandler partitionHandler =
            new DataSourceWriterContextPartitionHandler(
                ctx.createWriterContextFactory(), System.currentTimeMillis());

        JavaRDD<WriterCommitMessageContext> writerCommitMessagesRDD =
            rowsRDD.mapPartitionsWithIndex(partitionHandler, false);
        WriterCommitMessageContext[] writerCommitMessages =
            writerCommitMessagesRDD.collect().toArray(new WriterCommitMessageContext[0]);
        if (writerCommitMessages.length == numPartitions) {
          List<Exception> errors =
              Arrays.stream(writerCommitMessages)
                  .filter(msg -> msg.getError().isPresent())
                  .map(msg -> msg.getError().get())
                  .collect(Collectors.toList());
          if (errors.isEmpty()) {
            // All is well, let's commit
            ctx.commit(writerCommitMessages);
          } else {
            // oops, has some errors
            if (errors.size() == 1) {
              logger.error(
                  "Encountered errors in one partition, aborting the write", errors.get(0));
            } else {
              logger.error(
                  "Encountered errors in "
                      + errors
                      + " partition(s), aborting the write. Errors listed below");
              int i = 1;
              for (Exception e : errors) {
                logger.error("Error #" + i, e);
                i++;
              }
            }
            throw new BigQueryConnectorException("Write error in partitions.", errors.get(0));
          }
        } else {
          // missing commit messages, so abort
          ctx.abort(writerCommitMessages);
          throw new BigQueryConnectorException(
              String.format(
                  "It seems that %s out of %s partitions have failed, aborting",
                  numPartitions - writerCommitMessages.length, numPartitions));
        }
      }
    } catch (Exception e) {
      ctx.abort(new WriterCommitMessageContext[] {});
      throw new BigQueryConnectorException("unexpected issue trying to save " + data, e);
    }
  }
}
