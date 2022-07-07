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

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.write.context.DataSourceWriterContext;
import com.google.cloud.spark.bigquery.write.context.WriterCommitMessageContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDataSourceWriterInsertableRelation extends BigQueryInsertableRelationBase {

  private static Logger logger =
      LoggerFactory.getLogger(BigQueryDataSourceWriterInsertableRelation.class);

  private final DataSourceWriterContext ctx;

  public BigQueryDataSourceWriterInsertableRelation(
      BigQueryClient bigQueryClient,
      SQLContext sqlContext,
      SparkBigQueryConfig config,
      DataSourceWriterContext ctx) {
    super(bigQueryClient, sqlContext, config);
    this.ctx = ctx;
  }

  @Override
  public void insert(Dataset<Row> data, boolean overwrite) {
    logger.debug("Inserting data={}, overwrite={}", data, overwrite);

    try {
      DataSourceWriterContextPartitionHandler partitionHandler =
          new DataSourceWriterContextPartitionHandler(
              ctx.createWriterContextFactory(), System.currentTimeMillis());

      JavaRDD<Row> rowsRDD = data.toJavaRDD();
      int numPartitions = rowsRDD.getNumPartitions();
      JavaRDD<WriterCommitMessageContext> writerCommitMessagesRDD =
          rowsRDD.mapPartitionsWithIndex(partitionHandler, true);
      WriterCommitMessageContext[] writerCommitMessages =
          writerCommitMessagesRDD.collect().toArray(new WriterCommitMessageContext[0]);
      if (writerCommitMessages.length == numPartitions) {
        ctx.commit(writerCommitMessages);
      } else {
        // missing commit messages, so abort
        logger.warn(
            "It seems that {} out of {} partitions have failed, aborting",
            numPartitions - writerCommitMessages.length,
            writerCommitMessages.length);
        ctx.abort(writerCommitMessages);
      }
    } catch (Exception e) {
      logger.warn("unexpected issue trying to save " + data, e);
      ctx.abort(new WriterCommitMessageContext[] {});
    }
  }
}
