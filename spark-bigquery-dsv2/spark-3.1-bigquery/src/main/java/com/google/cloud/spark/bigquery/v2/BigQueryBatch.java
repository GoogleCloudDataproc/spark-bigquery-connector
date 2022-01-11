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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class BigQueryBatch implements Batch {

  private BigQueryDataSourceReaderContext ctx;

  public BigQueryBatch(BigQueryDataSourceReaderContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    // As each result has another template type we cannot set this to the same variable and to share
    // code
    if (ctx.enableBatchRead()) {
      return ctx.planBatchInputPartitionContexts()
          .map(inputPartitionContext -> new BigQueryInputPartition(inputPartitionContext))
          .toArray(InputPartition[]::new);
    } else {
      return ctx.planInputPartitionContexts()
          .map(inputPartitionContext -> new BigQueryInputPartition(inputPartitionContext))
          .toArray(InputPartition[]::new);
    }
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new BigQueryPartitionReaderFactory();
  }
}
