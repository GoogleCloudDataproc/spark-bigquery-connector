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

import com.google.cloud.spark.bigquery.SupportsQueryPushdown;
import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import java.util.Optional;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

/**
 * Both Scan and ScanBuilder implementation, otherwise estimateStatistics() is not called due to bug
 * in DataSourceV2Relation
 */
public class BigQueryScanBuilder
    implements Scan,
        ScanBuilder,
        SupportsPushDownFilters,
        SupportsPushDownRequiredColumns,
        SupportsReportStatistics,
        SupportsQueryPushdown {

  private BigQueryDataSourceReaderContext ctx;

  public BigQueryScanBuilder(BigQueryDataSourceReaderContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public Scan build() {
    return this; // new BigQueryScan(ctx);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    return ctx.pushFilters(filters);
  }

  @Override
  public Filter[] pushedFilters() {
    return ctx.pushedFilters();
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
    // create read session
    return new BigQueryBatch(ctx);
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
    return ctx.getCombinedFilter();
  }
}
