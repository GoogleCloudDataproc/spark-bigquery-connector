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
package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.*;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.cloud.bigquery.connector.common.BigQueryErrorCode.UNSUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.joining;

// holds caches and mappings
// presto converts the dataset and table names to lower case, while BigQuery is case sensitive
// the mappings here keep the mappings
public class BigQueryClient {
  private final BigQuery bigQuery;
  private final Optional<String> materializationProject;
  private final Optional<String> materializationDataset;

  BigQueryClient(
      BigQuery bigQuery,
      Optional<String> materializationProject,
      Optional<String> materializationDataset) {
    this.bigQuery = bigQuery;
    this.materializationProject = materializationProject;
    this.materializationDataset = materializationDataset;
  }

  // return empty if no filters are used
  private static Optional<String> createWhereClause(String[] filters) {
    if (filters.length == 0) {
      return Optional.empty();
    }
    return Optional.of(Stream.of(filters).collect(Collectors.joining(") AND (", "(", ")")));
  }

  public TableInfo getTable(TableId tableId) {
    return bigQuery.getTable(tableId);
  }

  public boolean tableExists(TableId tableId) {
    return getTable(tableId) != null;
  }

  public Table createTable(TableId tableId, Schema schema) {
    TableInfo tableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.of(schema)).build();
    return bigQuery.create(tableInfo);
  }

  public Table createTempTable(TableId destinationTableId, Schema schema) {
    String tempProject = materializationProject.orElseGet(destinationTableId::getProject);
    String tempDataset = materializationDataset.orElseGet(destinationTableId::getDataset);
    TableId tempTableId =
        TableId.of(tempProject, tempDataset, destinationTableId.getTable() + System.nanoTime());
    // Build TableInfo with expiration time of one day from current epoch.
    TableInfo tableInfo =
        TableInfo.newBuilder(tempTableId, StandardTableDefinition.of(schema))
            .setExpirationTime(System.currentTimeMillis() + 86400000L)
            .build();
    return bigQuery.create(tableInfo);
  }

  public boolean deleteTable(TableId tableId) {
    return bigQuery.delete(tableId);
  }

  public Job overwriteDestinationWithTemporary(
      TableId temporaryTableId, TableId destinationTableId) {
    String queryFormat =
        "MERGE `%s`\n"
            + "USING (SELECT * FROM `%s`)\n"
            + "ON FALSE\n"
            + "WHEN NOT MATCHED THEN INSERT ROW\n"
            + "WHEN NOT MATCHED BY SOURCE THEN DELETE";

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(
                sqlFromFormat(queryFormat, destinationTableId, temporaryTableId))
            .setUseLegacySql(false)
            .build();

    return create(JobInfo.newBuilder(queryConfig).build());
  }

  String sqlFromFormat(String queryFormat, TableId destinationTableId, TableId temporaryTableId) {
    return String.format(
        queryFormat, fullTableName(destinationTableId), fullTableName(temporaryTableId));
  }

  public String createTablePathForBigQueryStorage(TableId tableId) {
    return String.format(
        "projects/%s/datasets/%s/tables/%s",
        tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }

  public TableInfo getSupportedTable(
      TableId tableId, boolean viewsEnabled, String viewEnabledParamName) {
    TableInfo table = getTable(tableId);
    if (table == null) {
      return null;
    }

    TableDefinition tableDefinition = table.getDefinition();
    TableDefinition.Type tableType = tableDefinition.getType();
    if (TableDefinition.Type.TABLE == tableType) {
      return table;
    }
    if (TableDefinition.Type.VIEW == tableType
        || TableDefinition.Type.MATERIALIZED_VIEW == tableType) {
      if (viewsEnabled) {
        return table;
      } else {
        throw new BigQueryConnectorException(
            UNSUPPORTED,
            format(
                "Views are not enabled. You can enable views by setting '%s' to true. Notice additional cost may occur.",
                viewEnabledParamName));
      }
    }
    // not regular table or a view
    throw new BigQueryConnectorException(
        UNSUPPORTED,
        format(
            "Table type '%s' of table '%s.%s' is not supported",
            tableType, table.getTableId().getDataset(), table.getTableId().getTable()));
  }

  DatasetId toDatasetId(TableId tableId) {
    return DatasetId.of(tableId.getProject(), tableId.getDataset());
  }

  String getProjectId() {
    return bigQuery.getOptions().getProjectId();
  }

  Iterable<Dataset> listDatasets(String projectId) {
    return bigQuery.listDatasets(projectId).iterateAll();
  }

  Iterable<Table> listTables(DatasetId datasetId, TableDefinition.Type... types) {
    Set<TableDefinition.Type> allowedTypes = ImmutableSet.copyOf(types);
    Iterable<Table> allTables = bigQuery.listTables(datasetId).iterateAll();
    return StreamSupport.stream(allTables.spliterator(), false)
        .filter(table -> allowedTypes.contains(table.getDefinition().getType()))
        .collect(toImmutableList());
  }

  TableId createDestinationTable(TableId tableId) {
    String project = materializationProject.orElse(tableId.getProject());
    String dataset = materializationDataset.orElse(tableId.getDataset());
    DatasetId datasetId = DatasetId.of(project, dataset);
    String name = format("_bqc_%s", randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
    return TableId.of(datasetId.getProject(), datasetId.getDataset(), name);
  }

  Table update(TableInfo table) {
    return bigQuery.update(table);
  }

  Job create(JobInfo jobInfo) {
    return bigQuery.create(jobInfo);
  }

  TableResult query(String sql) {
    try {
      return bigQuery.query(QueryJobConfiguration.of(sql));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new BigQueryException(
          BaseHttpServiceException.UNKNOWN_CODE, format("Failed to run the query [%s]", sql), e);
    }
  }

  String createSql(TableId table, ImmutableList<String> requiredColumns, String[] filters) {
    String columns =
        requiredColumns.isEmpty()
            ? "*"
            : requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));

    String whereClause = createWhereClause(filters).map(clause -> "WHERE " + clause).orElse("");

    return createSql(table, columns, filters);
  }

  // assuming the SELECT part is properly formatted, can be used to call functions such as COUNT and
  // SUM
  String createSql(TableId table, String formattedQuery, String[] filters) {
    String tableName = fullTableName(table);

    String whereClause = createWhereClause(filters).map(clause -> "WHERE " + clause).orElse("");

    return format("SELECT %s FROM `%s` %s", formattedQuery, tableName, whereClause);
  }

  String fullTableName(TableId tableId) {
    return format("%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }

  public long calculateTableSize(TableId tableId, Optional<String> filter) {
    return calculateTableSize(getTable(tableId), filter);
  }

  public long calculateTableSize(TableInfo tableInfo, Optional<String> filter) {
    try {
      TableDefinition.Type type = tableInfo.getDefinition().getType();
      if (type == TableDefinition.Type.TABLE && !filter.isPresent()) {
        return tableInfo.getNumRows().longValue();
      } else if (type == TableDefinition.Type.VIEW
          || type == TableDefinition.Type.MATERIALIZED_VIEW
          || (type == TableDefinition.Type.TABLE && filter.isPresent())) {
        // run a query
        String table = fullTableName(tableInfo.getTableId());
        String sql = format("SELECT COUNT(*) from `%s` WHERE %s", table, filter.get());
        TableResult result = bigQuery.query(QueryJobConfiguration.of(sql));
        return result.iterateAll().iterator().next().get(0).getLongValue();
      } else {
        throw new IllegalArgumentException(
            format(
                "Unsupported table type %s for table %s",
                type, fullTableName(tableInfo.getTableId())));
      }
    } catch (InterruptedException e) {
      throw new BigQueryConnectorException(
          "Querying table size was interrupted on the client side", e);
    }
  }
}
