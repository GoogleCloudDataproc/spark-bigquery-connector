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

import com.google.cloud.bigquery.TableId;
import com.google.cloud.spark.bigquery.SupportsQueryPushdown;
import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import java.util.Objects;
import java.util.Optional;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Both Scan and ScanBuilder implementation, otherwise estimateStatistics() is not called due to bug
 * in DataSourceV2Relation
 */
public class Spark31BigQueryScanBuilder
    implements Batch,
        Scan,
        ScanBuilder,
        SupportsPushDownFilters,
        SupportsPushDownRequiredColumns,
        SupportsReportStatistics,
        SupportsQueryPushdown {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  protected BigQueryDataSourceReaderContext ctx;
  protected InputPartition[] partitions;

  public Spark31BigQueryScanBuilder(BigQueryDataSourceReaderContext ctx) {
    this.ctx = ctx;
  }

  public TableId getTableId() {
    return ctx.getTableId();
  }

  @Override
  public Scan build() {
    return this; // new BigQueryScan(ctx);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    ctx.pushFilters(filters);
    // We tell Spark that all filters were unhandled, in order to trigger DPP if needed
    // The relevant filters (usually all of them) where pushed to the Read API by `ctx`
    return filters;
  }

  @Override
  public Filter[] pushedFilters() {
    // We tell Spark that all filters were pushable, in order to trigger bloom filter if needed
    return ctx.getAllFilters();
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    ctx.pruneColumns(requiredSchema);
  }

  @Override
  public StructType readSchema() {
    return ctx.readSchema();
  }

  @Override
  public String description() {
    return String.format("Reading table [%s]", ctx.getFullTableName());
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public Statistics estimateStatistics() {
    return new Spark3Statistics(ctx.estimateStatistics());
  }

  @Override
  public BigQueryRDDFactory getBigQueryRDDFactory() {
    return ctx.getBigQueryRddFactory();
  }

  @Override
  public Optional<String> getPushdownFilters() {
    // Return the combined filters (pushed + global) here since Spark 3.1 does not create a Filter
    // node in the LogicalPlan
    return ctx.getCombinedFilter();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Spark31BigQueryScanBuilder that = (Spark31BigQueryScanBuilder) o;
    return getTableId().equals(that.getTableId())
        && readSchema().equals(that.readSchema())
        && // compare Spark schemas to ignore field ids
        getPushdownFilters().equals(that.getPushdownFilters());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTableId(), readSchema(), getPushdownFilters());
  }

  @Override
  public InputPartition[] planInputPartitions() {
    // As each result has another template type we cannot set this to the same variable and to share
    // code
    if (partitions != null) {
      return partitions;
    }
    if (ctx.enableBatchRead()) {
      partitions =
          ctx.planBatchInputPartitionContexts()
              .map(inputPartitionContext -> new BigQueryInputPartition(inputPartitionContext))
              .toArray(InputPartition[]::new);
    } else {
      partitions =
          ctx.planInputPartitionContexts()
              .map(inputPartitionContext -> new BigQueryInputPartition(inputPartitionContext))
              .toArray(InputPartition[]::new);
    }
    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new BigQueryPartitionReaderFactory();
  }
}
