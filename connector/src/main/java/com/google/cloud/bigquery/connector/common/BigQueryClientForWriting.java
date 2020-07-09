package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.*;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

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

// A BigQueryClient for writing purposes (can delete a table, etc.)
public class BigQueryClientForWriting {
    private final BigQuery bigQuery;
    private final Optional<String> materializationProject;
    private final Optional<String> materializationDataset;

    BigQueryClientForWriting(
            BigQuery bigQuery,
            Optional<String> materializationProject,
            Optional<String> materializationDataset) {
        this.bigQuery = bigQuery;
        this.materializationProject = materializationProject;
        this.materializationDataset = materializationDataset;
    }

    public TableInfo getTable(TableId tableId) {
        return bigQuery.getTable(tableId);
    }

    public boolean tableExists(TableId tableId) {
        return getTable(tableId) != null;
    }

    public Table createTable(TableInfo tableInfo) {
        return bigQuery.create(tableInfo);
    }

    public boolean deleteTable(TableId tableId) {
        return bigQuery.delete(tableId);
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
