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

import com.google.cloud.spark.bigquery.SupportsCompleteQueryPushdown;
import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class BigQueryDataSourceReader
    implements DataSourceReader,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsReportStatistics,
        SupportsScanColumnarBatch,
        SupportsCompleteQueryPushdown {

  private BigQueryDataSourceReaderContext context;

  public BigQueryDataSourceReader(BigQueryDataSourceReaderContext context) {
    this.context = context;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    return context.pushFilters(filters);
  }

  @Override
  public Filter[] pushedFilters() {
    return context.pushedFilters();
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    context.pruneColumns(requiredSchema);
  }

  @Override
  public Statistics estimateStatistics() {
    return new Spark24Statistics(context.estimateStatistics());
  }

  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
    return context
        .planBatchInputPartitionContexts()
        .map(ctx -> new Spark24InputPartition<ColumnarBatch>(ctx))
        .collect(Collectors.toList());
  }

  @Override
  public StructType readSchema() {
    return context.readSchema();
  }

  @Override
  public List<InputPartition<InternalRow>> planInputPartitions() {
    return context
        .planInputPartitionContexts()
        .map(ctx -> new Spark24InputPartition<InternalRow>(ctx))
        .collect(Collectors.toList());
  }

  @Override
  public boolean enableBatchRead() {
    return context.enableBatchRead();
  }

  public BigQueryRDDFactory getBigQueryRDDFactory() {
    return context.getBigQueryRddFactory();
  }
}
