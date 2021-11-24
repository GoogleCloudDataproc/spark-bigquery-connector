/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDataSourceReader;
import java.util.*;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public class BigQueryScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
  private GenericBigQueryDataSourceReader dataSourceReader;

  BigQueryScanBuilder(
      TableInfo table,
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      ReadSessionCreatorConfig readSessionCreatorConfig,
      Optional<String> globalFilter,
      Optional<StructType> schema,
      String applicationId) {
    this.dataSourceReader =
        new GenericBigQueryDataSourceReader(
            table,
            readSessionCreatorConfig,
            bigQueryClient,
            bigQueryReadClientFactory,
            tracerFactory,
            globalFilter,
            schema,
            applicationId);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    return this.dataSourceReader.pushFilters(filters);
  }

  @Override
  public Filter[] pushedFilters() {
    return this.dataSourceReader.getPushedFilters();
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.dataSourceReader.pruneColumns(requiredSchema);
  }

  @Override
  public Scan build() {
    return new BigQueryBatchScan(
        this.dataSourceReader.getTable(),
        this.dataSourceReader.getTableId(),
        this.dataSourceReader.getSchema(),
        this.dataSourceReader.getUserProvidedSchema(),
        this.dataSourceReader.getFields(),
        this.dataSourceReader.getReadSessionCreatorConfig(),
        this.dataSourceReader.getBigQueryClient(),
        this.dataSourceReader.getBigQueryReadClientFactory(),
        this.dataSourceReader.getBigQueryTracerFactory(),
        this.dataSourceReader.getReadSessionCreator(),
        this.dataSourceReader.getGlobalFilter(),
        this.dataSourceReader.getPushedFilters(),
        this.dataSourceReader.getApplicationId());
  }
}
