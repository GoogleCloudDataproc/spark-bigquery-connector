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
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    private final ConcurrentMap<TableId, TableId> tableIds = new ConcurrentHashMap<>();
    private final ConcurrentMap<DatasetId, DatasetId> datasetIds = new ConcurrentHashMap<>();

    BigQueryClient(BigQuery bigQuery, Optional<String> materializationProject, Optional<String> materializationDataset) {
        this.bigQuery = bigQuery;
        this.materializationProject = materializationProject;
        this.materializationDataset = materializationDataset;
    }

    // return empty if no filters are used
    private static Optional<String> createWhereClause(String[] filters) {
        return Optional.empty();
    }

    public TableInfo getTable(TableId tableId) {
        TableId bigQueryTableId = tableIds.get(tableId);
        Table table = bigQuery.getTable(bigQueryTableId != null ? bigQueryTableId : tableId);
        if (table != null) {
            tableIds.putIfAbsent(tableId, table.getTableId());
            datasetIds.putIfAbsent(toDatasetId(tableId), toDatasetId(table.getTableId()));
        }
        return table;
    }

    public TableInfo getSupportedTable(TableId tableId, boolean viewsEnabled, String viewEnabledParamName) {
        TableInfo table = getTable(tableId);
        if (table == null) {
            return null;
        }

        TableDefinition tableDefinition = table.getDefinition();
        TableDefinition.Type tableType = tableDefinition.getType();
        if (TableDefinition.Type.TABLE == tableType) {
            return table;
        }
        if (TableDefinition.Type.VIEW == tableType) {
            if (viewsEnabled) {
                return table;
            } else {
                throw new BigQueryConnectorException(UNSUPPORTED, format(
                        "Views are not enabled. You can enable views by setting '%s' to true. Notice additional cost may occur.",
                        viewEnabledParamName));
            }
        }
        // not regular table or a view
        throw new BigQueryConnectorException(UNSUPPORTED, format("Table type '%s' of table '%s.%s' is not supported",
                tableType, table.getTableId().getDataset(), table.getTableId().getTable()));
    }

    DatasetId toDatasetId(TableId tableId) {
        return DatasetId.of(tableId.getProject(), tableId.getDataset());
    }

    String getProjectId() {
        return bigQuery.getOptions().getProjectId();
    }

    Iterable<Dataset> listDatasets(String projectId) {
        final Iterator<Dataset> datasets = bigQuery.listDatasets(projectId).iterateAll().iterator();
        return () -> Iterators.transform(datasets, this::addDataSetMappingIfNeeded);
    }

    Iterable<Table> listTables(DatasetId datasetId, TableDefinition.Type... types) {
        Set<TableDefinition.Type> allowedTypes = ImmutableSet.copyOf(types);
        DatasetId bigQueryDatasetId = datasetIds.getOrDefault(datasetId, datasetId);
        Iterable<Table> allTables = bigQuery.listTables(bigQueryDatasetId).iterateAll();
        return StreamSupport.stream(allTables.spliterator(), false)
                .filter(table -> allowedTypes.contains(table.getDefinition().getType()))
                .collect(toImmutableList());
    }

    private Dataset addDataSetMappingIfNeeded(Dataset dataset) {
        DatasetId bigQueryDatasetId = dataset.getDatasetId();
        DatasetId prestoDatasetId = DatasetId.of(bigQueryDatasetId.getProject(), bigQueryDatasetId.getDataset().toLowerCase(ENGLISH));
        datasetIds.putIfAbsent(prestoDatasetId, bigQueryDatasetId);
        return dataset;
    }

    TableId createDestinationTable(TableId tableId) {
        String project = materializationProject.orElse(tableId.getProject());
        String dataset = materializationDataset.orElse(tableId.getDataset());
        DatasetId datasetId = mapIfNeeded(project, dataset);
        UUID uuid = randomUUID();
        String name = format("_pbc_%s", randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
        return TableId.of(datasetId.getProject(), datasetId.getDataset(), name);
    }

    private DatasetId mapIfNeeded(String project, String dataset) {
        DatasetId datasetId = DatasetId.of(project, dataset);
        return datasetIds.getOrDefault(datasetId, datasetId);
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
            throw new BigQueryException(BaseHttpServiceException.UNKNOWN_CODE, format("Failed to run the query [%s]", sql), e);
        }
    }

    String createSql(TableId table, ImmutableList<String> requiredColumns, String[] filters) {
        String columns = requiredColumns.isEmpty() ? "*" :
                requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));

        String whereClause = createWhereClause(filters)
                .map(clause -> "WHERE " + clause)
                .orElse("");

        return createSql(table, columns, filters);
    }

    // assuming the SELECT part is properly formatted, can be used to call functions such as COUNT and SUM
    String createSql(TableId table, String formatedQuery, String[] filters) {
        String tableName = fullTableName(table);

        String whereClause = createWhereClause(filters)
                .map(clause -> "WHERE " + clause)
                .orElse("");

        return format("SELECT %s FROM `%s` %s", formatedQuery, tableName, whereClause);
    }

    String fullTableName(TableId tableId) {
        tableId = tableIds.getOrDefault(tableId, tableId);
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
            } else if (type == TableDefinition.Type.VIEW ||
                    (type == TableDefinition.Type.TABLE && filter.isPresent())) {
                // run a query
                String table = fullTableName(tableInfo.getTableId());
                String sql = format("SELECT COUNT(*) from `%s` WHERE %s", table, filter.get());
                TableResult result = bigQuery.query(QueryJobConfiguration.of(sql));
                return result.iterateAll().iterator().next().get(0).getLongValue();
            } else {
                throw new IllegalArgumentException(format("Unsupported table type %s for table %s",
                        type, fullTableName(tableInfo.getTableId())));
            }
        } catch (InterruptedException e) {
            throw new BigQueryConnectorException("Querying table size was interrupted on the client side", e);
        }
    }
}
