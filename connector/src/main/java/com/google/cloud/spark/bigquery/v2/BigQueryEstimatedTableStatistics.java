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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.sources.v2.reader.Statistics;

import java.util.Optional;
import java.util.OptionalLong;

class BigQueryEstimatedTableStatistics implements Statistics {

  private static BigQueryEstimatedTableStatistics UNKNOWN_STATISTICS =
      new BigQueryEstimatedTableStatistics(OptionalLong.empty(), OptionalLong.empty());

  private OptionalLong numRows;
  private OptionalLong sizeInBytes;

  private BigQueryEstimatedTableStatistics(OptionalLong numRows, OptionalLong sizeInBytes) {
    this.numRows = numRows;
    this.sizeInBytes = sizeInBytes;
  }

  static Builder newBuilder(TableInfo table, BigQueryClient bigQueryClient) {
    return new Builder(table, bigQueryClient);
  }

  @Override
  public OptionalLong sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public OptionalLong numRows() {
    return numRows;
  }

  static class Builder {
    final TableInfo table;
    final BigQueryClient bigQueryClient;
    Optional<String> filter = Optional.empty();
    Optional<ImmutableList<String>> selectedColumns = Optional.empty();

    public Builder(TableInfo table, BigQueryClient bigQueryClient) {
      this.table = table;
      this.bigQueryClient = bigQueryClient;
    }

    Builder setFilter(Optional<String> filter) {
      this.filter = filter;
      return this;
    }

    Builder setSelectedColumns(Optional<ImmutableList<String>> selectedColumns) {
      this.selectedColumns = selectedColumns;
      return this;
    }

    BigQueryEstimatedTableStatistics build() {
      // step 1 - verify this is a standard table (not view)
      if (table.getDefinition().getType() != TableDefinition.Type.TABLE) {
        return UNKNOWN_STATISTICS;
      }
      // step 2 - if there is a filter, then issue a query to find the number of rows by the query.
      // Otherwise, take it from the tableInfo.
      long numberOfRowsInTable = table.getNumRows().longValue();
      long numberOfRows = filter.map(this::getNumberOfRowsWithFilter).orElse(numberOfRowsInTable);
      // Step 3 - if there where selected columns, estimate the size of the table for the entire
      // table. Otherwise, take it from the tableInfo.
      long projectedTableSizeInBytes =
          selectedColumns
              .map(this::getProjectedTableSizeInBytesFromColumns)
              .orElse(table.getNumBytes());
      long sizeInBytes =
          (long) (1.0 * projectedTableSizeInBytes * numberOfRows / numberOfRowsInTable);

      // step 4 - return the statistics
      return new BigQueryEstimatedTableStatistics(
          OptionalLong.of(numberOfRows), OptionalLong.of(sizeInBytes));
    }

    private long getNumberOfRowsWithFilter(String actualFilter) {
      long numberOfRows = bigQueryClient.calculateTableSize(table, Optional.of(actualFilter));
      return numberOfRows;
    }

    private long getProjectedTableSizeInBytesFromColumns(ImmutableList<String> columns) {
      String sql = bigQueryClient.createSql(table.getTableId(), columns, new String[] {});
      Job dryRunJob = bigQueryClient.dryRunQuery(sql);
      long projectedTableSizeInBytes =
          ((JobStatistics.QueryStatistics) dryRunJob.getStatistics()).getTotalBytesProcessed();
      return projectedTableSizeInBytes;
    }
  }
}
