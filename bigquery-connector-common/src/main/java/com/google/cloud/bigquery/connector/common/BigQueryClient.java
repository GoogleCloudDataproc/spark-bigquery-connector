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

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.getQueryForRangePartitionedTable;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.getQueryForTimePartitionedTable;

import com.google.cloud.BaseServiceException;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

// holds caches and mappings
// presto converts the dataset and table names to lower case, while BigQuery is case sensitive
// the mappings here keep the mappings
public class BigQueryClient {
  private static final Logger log = LoggerFactory.getLogger(BigQueryClient.class);

  private final BigQuery bigQuery;
  private final Cache<String, TableInfo> destinationTableCache;
  private final Optional<String> materializationProject;
  private final Optional<String> materializationDataset;
  private final JobConfigurationFactory jobConfigurationFactory;

  public BigQueryClient(
      BigQuery bigQuery,
      Optional<String> materializationProject,
      Optional<String> materializationDataset,
      Cache<String, TableInfo> destinationTableCache,
      Map<String, String> labels,
      Priority queryJobPriority) {
    this.bigQuery = bigQuery;
    this.materializationProject = materializationProject;
    this.materializationDataset = materializationDataset;
    this.destinationTableCache = destinationTableCache;
    this.jobConfigurationFactory = new JobConfigurationFactory(labels, queryJobPriority);
  }

  /**
   * Waits for a BigQuery Job to complete: this is a blocking function.
   *
   * @param job The {@code Job} to keep track of.
   */
  public static void waitForJob(Job job) {
    try {
      Job completedJob =
          job.waitFor(
              RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
              RetryOption.totalTimeout(Duration.ofMinutes(3)));
      if (completedJob == null && completedJob.getStatus().getError() != null) {
        throw new UncheckedIOException(
            new IOException(completedJob.getStatus().getError().toString()));
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Could not copy table from temporary sink to destination table.", e);
    }
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

  /**
   * Checks whether the requested table exists in BigQuery.
   *
   * @param tableId The TableId of the requested table in BigQuery
   * @return True if the requested table exists in BigQuery, false otherwise.
   */
  public boolean tableExists(TableId tableId) {
    return getTable(tableId) != null;
  }

  /**
   * Creates an empty table in BigQuery.
   *
   * @param tableId The TableId of the table to be created.
   * @param schema The Schema of the table to be created.
   * @return The {@code Table} object representing the table that was created.
   *     <p>public TableInfo createTable(TableId tableId, Schema schema) { return
   *     createTable(tableId, schema, Optional.empty()); }
   */

  /**
   * Creates an empty table in BigQuery.
   *
   * @param tableId The TableId of the table to be created.
   * @param schema The Schema of the table to be created.
   * @param options Allows configuring the created table
   * @return The {@code Table} object representing the table that was created.
   */
  public TableInfo createTable(TableId tableId, Schema schema, CreateTableOptions options) {
    TableInfo.Builder tableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.of(schema));
    options
        .getKmsKeyName()
        .ifPresent(
            keyName ->
                tableInfo.setEncryptionConfiguration(
                    EncryptionConfiguration.newBuilder().setKmsKeyName(keyName).build()));
    if (!options.getBigQueryTableLabels().isEmpty()) {
      tableInfo.setLabels(options.getBigQueryTableLabels());
    }
    return bigQuery.create(tableInfo.build());
  }

  /**
   * Creates a temporary table with a time-to-live of 1 day, and the same location as the
   * destination table; the temporary table will have the same name as the destination table, with
   * the current time in milliseconds appended to it; useful for holding temporary data in order to
   * overwrite the destination table.
   *
   * @param destinationTableId The TableId of the eventual destination for the data going into the
   *     temporary table.
   * @param schema The Schema of the destination / temporary table.
   * @return The {@code Table} object representing the created temporary table.
   */
  public TableInfo createTempTable(TableId destinationTableId, Schema schema) {
    TableId tempTableId = createTempTableId(destinationTableId);
    // Build TableInfo with expiration time of one day from current epoch.
    TableInfo tableInfo =
        TableInfo.newBuilder(tempTableId, StandardTableDefinition.of(schema))
            .setExpirationTime(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1))
            .build();
    return bigQuery.create(tableInfo);
  }

  public TableId createTempTableId(TableId destinationTableId) {
    String tempProject = materializationProject.orElseGet(destinationTableId::getProject);
    String tempDataset = materializationDataset.orElseGet(destinationTableId::getDataset);
    String tableName = destinationTableId.getTable() + System.nanoTime();
    TableId tempTableId =
        tempProject == null
            ? TableId.of(tempDataset, tableName)
            : TableId.of(tempProject, tempDataset, tableName);
    return tempTableId;
  }

  /**
   * Deletes this table in BigQuery.
   *
   * @param tableId The TableId of the table to be deleted.
   * @return True if the operation was successful, false otherwise.
   */
  public boolean deleteTable(TableId tableId) {
    return bigQuery.delete(tableId);
  }

  private Job copyData(
      TableId sourceTableId,
      TableId destinationTableId,
      JobInfo.WriteDisposition writeDisposition) {
    String queryFormat = "SELECT * FROM `%s`";
    String temporaryTableName = fullTableName(sourceTableId);
    String sqlQuery = String.format(queryFormat, temporaryTableName);
    QueryJobConfiguration queryConfig =
        jobConfigurationFactory
            .createQueryJobConfigurationBuilder(sqlQuery, Collections.emptyMap())
            .setUseLegacySql(false)
            .setDestinationTable(destinationTableId)
            .setWriteDisposition(writeDisposition)
            .build();

    return create(JobInfo.newBuilder(queryConfig).build());
  }

  /**
   * Overwrites the partitions of the destination table, using the partitions from the given
   * temporary table, transactionally.
   *
   * @param temporaryTableId The {@code TableId} representing the temporary-table.
   * @param destinationTableId The {@code TableId} representing the destination table.
   * @return The {@code Job} object representing this operation (which can be tracked to wait until
   *     it has finished successfully).
   */
  public Job overwriteDestinationWithTemporaryDynamicPartitons(
      TableId temporaryTableId, TableId destinationTableId) {

    TableDefinition destinationDefinition = getTable(destinationTableId).getDefinition();
    String sqlQuery = null;
    if (destinationDefinition instanceof StandardTableDefinition) {
      String destinationTableName = fullTableName(destinationTableId);
      String temporaryTableName = fullTableName(temporaryTableId);
      StandardTableDefinition sdt = (StandardTableDefinition) destinationDefinition;

      TimePartitioning timePartitioning = sdt.getTimePartitioning();
      if (timePartitioning != null) {
        sqlQuery =
            getQueryForTimePartitionedTable(
                destinationTableName, temporaryTableName, sdt, timePartitioning);
      }

      RangePartitioning rangePartitioning = sdt.getRangePartitioning();
      if (rangePartitioning != null) {
        sqlQuery =
            getQueryForRangePartitionedTable(
                destinationTableName, temporaryTableName, sdt, rangePartitioning);
      }
      if (sqlQuery != null) {
        QueryJobConfiguration queryConfig =
            jobConfigurationFactory
                .createQueryJobConfigurationBuilder(sqlQuery, Collections.emptyMap())
                .setUseLegacySql(false)
                .build();

        return create(JobInfo.newBuilder(queryConfig).build());
      }
    }

    // no partitioning default to statndard overwrite
    return overwriteDestinationWithTemporary(temporaryTableId, destinationTableId);
  }

  /**
   * Overwrites the given destination table, with all the data from the given temporary table,
   * transactionally.
   *
   * @param temporaryTableId The {@code TableId} representing the temporary-table.
   * @param destinationTableId The {@code TableId} representing the destination table.
   * @return The {@code Job} object representing this operation (which can be tracked to wait until
   *     it has finished successfully).
   */
  public Job overwriteDestinationWithTemporary(
      TableId temporaryTableId, TableId destinationTableId) {
    String queryFormat =
        "MERGE `%s`\n"
            + "USING (SELECT * FROM `%s`)\n"
            + "ON FALSE\n"
            + "WHEN NOT MATCHED THEN INSERT ROW\n"
            + "WHEN NOT MATCHED BY SOURCE THEN DELETE";

    String destinationTableName = fullTableName(destinationTableId);
    String temporaryTableName = fullTableName(temporaryTableId);
    String sqlQuery = String.format(queryFormat, destinationTableName, temporaryTableName);
    QueryJobConfiguration queryConfig =
        jobConfigurationFactory
            .createQueryJobConfigurationBuilder(sqlQuery, Collections.emptyMap())
            .setUseLegacySql(false)
            .build();

    return create(JobInfo.newBuilder(queryConfig).build());
  }

  /**
   * Appends all the data from the given temporary table, to the given destination table,
   * transactionally.
   *
   * @param temporaryTableId The {@code TableId} representing the temporary-table.
   * @param destinationTableId The {@code TableId} representing the destination table.
   * @return The {@code Job} object representing this operation (which can be tracked to wait until
   *     it has finished successfully).
   */
  public Job appendDestinationWithTemporary(TableId temporaryTableId, TableId destinationTableId) {
    return copyData(temporaryTableId, destinationTableId, JobInfo.WriteDisposition.WRITE_APPEND);
  }

  /**
   * Creates a String appropriately formatted for BigQuery Storage Write API representing the given
   * table.
   *
   * @param tableId The {@code TableId} representing the given object.
   * @return The formatted String.
   */
  public String createTablePathForBigQueryStorage(TableId tableId) {
    Preconditions.checkNotNull(tableId, "tableId cannot be null");
    // We need the full path for the createWriteStream method. We used to have it by creating the
    // table and then taking its full tableId, but that caused an issue with the ErrorIfExists
    // implementation (now the check, done in another place is positive). To solve it, we do what
    // the BigQuery client does on Table ID with no project - take the BigQuery client own project
    // ID.  This gives us the same behavior but allows us to defer the table creation to the last
    // minute.
    String project = tableId.getProject() != null ? tableId.getProject() : getProjectId();
    return String.format(
        "projects/%s/datasets/%s/tables/%s", project, tableId.getDataset(), tableId.getTable());
  }

  public TableInfo getReadTable(ReadTableOptions options) {
    Optional<String> query = options.query();
    // first, let check if this is a query
    if (query.isPresent()) {
      // in this case, let's materialize it and use it as the table
      validateViewsEnabled(options);
      String sql = query.get();
      return materializeQueryToTable(sql, options.expirationTimeInMinutes());
    }

    TableInfo table = getTable(options.tableId());
    if (table == null) {
      return null;
    }

    TableDefinition tableDefinition = table.getDefinition();
    TableDefinition.Type tableType = tableDefinition.getType();
    if (TableDefinition.Type.TABLE == tableType
        || TableDefinition.Type.EXTERNAL == tableType
        || TableDefinition.Type.SNAPSHOT == tableType) {
      return table;
    }
    if (TableDefinition.Type.VIEW == tableType
        || TableDefinition.Type.MATERIALIZED_VIEW == tableType) {
      validateViewsEnabled(options);
      // view materialization is done in a lazy manner, so it can occur only when the data is read
      return table;
    }
    // not regular table or a view
    throw new BigQueryConnectorException(
        BigQueryErrorCode.UNSUPPORTED,
        String.format(
            "Table type '%s' of table '%s.%s' is not supported",
            tableType, table.getTableId().getDataset(), table.getTableId().getTable()));
  }

  private void validateViewsEnabled(ReadTableOptions options) {
    if (!options.viewsEnabled()) {
      throw new BigQueryConnectorException(
          BigQueryErrorCode.UNSUPPORTED,
          String.format(
              "Views are not enabled. You can enable views by setting '%s' to true. Notice"
                  + " additional cost may occur.",
              options.viewEnabledParamName()));
    }
  }

  DatasetId toDatasetId(TableId tableId) {
    return DatasetId.of(tableId.getProject(), tableId.getDataset());
  }

  public String getProjectId() {
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
        .collect(ImmutableList.toImmutableList());
  }

  TableId createDestinationTable(
      Optional<String> referenceProject, Optional<String> referenceDataset) {
    String project = materializationProject.orElse(referenceProject.orElse(null));
    String dataset = materializationDataset.orElse(referenceDataset.orElse(null));
    String name =
        String.format(
            "_bqc_%s", UUID.randomUUID().toString().toLowerCase(Locale.ENGLISH).replace("-", ""));
    return project == null ? TableId.of(dataset, name) : TableId.of(project, dataset, name);
  }

  public Table update(TableInfo table) {
    return bigQuery.update(table);
  }

  public Job createAndWaitFor(JobConfiguration.Builder jobConfiguration) {
    return createAndWaitFor(jobConfiguration.build());
  }

  public Job createAndWaitFor(JobConfiguration jobConfiguration) {
    JobInfo jobInfo = JobInfo.of(jobConfiguration);
    Job job = bigQuery.create(jobInfo);
    Job returnedJob = null;

    log.info("Submitted job {}. jobId: {}", jobConfiguration, job.getJobId());
    try {
      Job completedJob = job.waitFor();
      if (completedJob == null) {
        throw new BigQueryException(
            BaseHttpServiceException.UNKNOWN_CODE,
            String.format("Failed to run the job [%s], got null back", job));
      }
      if (completedJob.getStatus().getError() != null) {
        throw new BigQueryException(
            BaseHttpServiceException.UNKNOWN_CODE,
            String.format(
                "Failed to run the job [%s], due to '%s'", completedJob.getStatus().getError()));
      }
      return completedJob;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new BigQueryException(
          BaseHttpServiceException.UNKNOWN_CODE,
          String.format("Failed to run the job [%s], task was interrupted", job),
          e);
    }
  }

  Job create(JobInfo jobInfo) {
    return bigQuery.create(jobInfo);
  }

  public TableResult query(String sql) {
    try {
      return bigQuery.query(
          jobConfigurationFactory
              .createQueryJobConfigurationBuilder(sql, Collections.emptyMap())
              .build());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new BigQueryException(
          BaseHttpServiceException.UNKNOWN_CODE,
          String.format("Failed to run the query [%s]", sql),
          e);
    }
  }

  String createSql(TableId table, ImmutableList<String> requiredColumns, String[] filters) {
    String columns =
        requiredColumns.isEmpty()
            ? "*"
            : requiredColumns.stream()
                .map(column -> String.format("`%s`", column))
                .collect(Collectors.joining(","));

    return createSql(table, columns, filters);
  }

  // assuming the SELECT part is properly formatted, can be used to call functions such as COUNT and
  // SUM
  String createSql(TableId table, String formattedQuery, String[] filters) {
    String tableName = fullTableName(table);

    String whereClause = createWhereClause(filters).map(clause -> "WHERE " + clause).orElse("");

    return String.format("SELECT %s FROM `%s` %s", formattedQuery, tableName, whereClause);
  }

  public static String fullTableName(TableId tableId) {
    if (tableId.getProject() == null) {
      return String.format("%s.%s", tableId.getDataset(), tableId.getTable());
    } else {
      return String.format(
          "%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }
  }

  public long calculateTableSize(TableId tableId, Optional<String> filter) {
    return calculateTableSize(getTable(tableId), filter);
  }

  public long calculateTableSize(TableInfo tableInfo, Optional<String> filter) {
    TableDefinition.Type type = tableInfo.getDefinition().getType();
    if (type == TableDefinition.Type.TABLE && !filter.isPresent()) {
      return tableInfo.getNumRows().longValue();
    } else if (type == TableDefinition.Type.EXTERNAL && !filter.isPresent()) {
      String table = fullTableName(tableInfo.getTableId());
      return getNumberOfRows(String.format("SELECT COUNT(*) from `%s`", table));
    } else if (type == TableDefinition.Type.VIEW
        || type == TableDefinition.Type.MATERIALIZED_VIEW
        || ((type == TableDefinition.Type.TABLE || type == TableDefinition.Type.EXTERNAL)
            && filter.isPresent())) {
      // run a query
      String table = fullTableName(tableInfo.getTableId());
      String whereClause = filter.map(f -> "WHERE " + f).orElse("");
      return getNumberOfRows(String.format("SELECT COUNT(*) from `%s` %s", table, whereClause));
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported table type %s for table %s",
              type, fullTableName(tableInfo.getTableId())));
    }
  }

  private long getNumberOfRows(String sql) {
    TableResult result = query(sql);
    long numberOfRows = result.iterateAll().iterator().next().get(0).getLongValue();
    return numberOfRows;
  }

  /**
   * Runs the provided query on BigQuery and saves the result in a temporary table.
   *
   * @param querySql the query to be run
   * @param expirationTimeInMinutes the time in minutes until the table is expired and auto-deleted
   * @return a reference to the table
   */
  public TableInfo materializeQueryToTable(String querySql, int expirationTimeInMinutes) {
    TableId tableId = createDestinationTable(Optional.empty(), Optional.empty());
    return materializeTable(querySql, tableId, expirationTimeInMinutes);
  }

  /**
   * Runs the provided query on BigQuery and saves the result in a temporary table.
   *
   * @param querySql the query to be run
   * @param expirationTimeInMinutes the time in minutes until the table is expired and auto-deleted
   * @param additionalQueryJobLabels the labels to insert on the query job
   * @return a reference to the table
   */
  public TableInfo materializeQueryToTable(
      String querySql, int expirationTimeInMinutes, Map<String, String> additionalQueryJobLabels) {
    TableId destinationTableId = createDestinationTable(Optional.empty(), Optional.empty());
    DestinationTableBuilder tableBuilder =
        new DestinationTableBuilder(
            this,
            querySql,
            destinationTableId,
            expirationTimeInMinutes,
            jobConfigurationFactory,
            additionalQueryJobLabels);

    return materializeTable(querySql, tableBuilder);
  }

  /**
   * Runs the provided query on BigQuery and saves the result in a temporary table. This method is
   * intended to be used to materialize views, so the view location (based on its TableId) is taken
   * as a location for the temporary table, removing the need to set the materializationProject and
   * materializationDataset properties
   *
   * @param querySql the query to be run
   * @param viewId the view the query came from
   * @param expirationTimeInMinutes the time in hours until the table is expired and auto-deleted
   * @return a reference to the table
   */
  public TableInfo materializeViewToTable(
      String querySql, TableId viewId, int expirationTimeInMinutes) {
    TableId tableId =
        createDestinationTable(
            Optional.ofNullable(viewId.getProject()), Optional.ofNullable(viewId.getDataset()));
    return materializeTable(querySql, tableId, expirationTimeInMinutes);
  }

  private TableInfo materializeTable(
      String querySql, TableId destinationTableId, int expirationTimeInMinutes) {
    try {
      return destinationTableCache.get(
          querySql,
          new DestinationTableBuilder(
              this,
              querySql,
              destinationTableId,
              expirationTimeInMinutes,
              jobConfigurationFactory,
              Collections.emptyMap()));
    } catch (Exception e) {
      throw new BigQueryConnectorException(
          BigQueryErrorCode.BIGQUERY_VIEW_DESTINATION_TABLE_CREATION_FAILED,
          String.format(
              "Error creating destination table using the following query: [%s]", querySql),
          e);
    }
  }

  private TableInfo materializeTable(
      String querySql, DestinationTableBuilder destinationTableBuilder) {
    try {
      return destinationTableCache.get(querySql, destinationTableBuilder);
    } catch (Exception e) {
      throw new BigQueryConnectorException(
          BigQueryErrorCode.BIGQUERY_VIEW_DESTINATION_TABLE_CREATION_FAILED,
          String.format(
              "Error creating destination table using the following query: [%s]", querySql),
          e);
    }
  }

  public void loadDataIntoTable(
      LoadDataOptions options,
      List<String> sourceUris,
      FormatOptions formatOptions,
      JobInfo.WriteDisposition writeDisposition,
      Optional<Schema> schema) {
    LoadJobConfiguration.Builder jobConfiguration =
        jobConfigurationFactory
            .createLoadJobConfigurationBuilder(options, sourceUris, formatOptions)
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .setWriteDisposition(writeDisposition);

    if (schema.isPresent()) {
      jobConfiguration.setSchema(schema.get());
    } else {
      // no schema, probably table does not exist
      jobConfiguration.setAutodetect(true);
    }

    options.getCreateDisposition().ifPresent(jobConfiguration::setCreateDisposition);

    if (options.getPartitionField().isPresent() || options.getPartitionType().isPresent()) {
      TimePartitioning.Builder timePartitionBuilder =
          TimePartitioning.newBuilder(options.getPartitionTypeOrDefault());
      options.getPartitionExpirationMs().ifPresent(timePartitionBuilder::setExpirationMs);
      options
          .getPartitionRequireFilter()
          .ifPresent(timePartitionBuilder::setRequirePartitionFilter);
      options.getPartitionField().ifPresent(timePartitionBuilder::setField);
      jobConfiguration.setTimePartitioning(timePartitionBuilder.build());
    }
    if (options.getPartitionField().isPresent() && options.getPartitionRange().isPresent()) {
      RangePartitioning.Builder rangePartitionBuilder = RangePartitioning.newBuilder();
      options.getPartitionField().ifPresent(rangePartitionBuilder::setField);
      options.getPartitionRange().ifPresent(rangePartitionBuilder::setRange);
      jobConfiguration.setRangePartitioning(rangePartitionBuilder.build());
    }

    options
        .getClusteredFields()
        .ifPresent(
            clusteredFields -> {
              Clustering clustering = Clustering.newBuilder().setFields(clusteredFields).build();
              jobConfiguration.setClustering(clustering);
            });

    if (options.isUseAvroLogicalTypes()) {
      jobConfiguration.setUseAvroLogicalTypes(true);
    }

    if (!options.getDecimalTargetTypes().isEmpty()) {
      jobConfiguration.setDecimalTargetTypes(options.getDecimalTargetTypes());
    }

    if (!options.getLoadSchemaUpdateOptions().isEmpty()) {
      jobConfiguration.setSchemaUpdateOptions(options.getLoadSchemaUpdateOptions());
    }

    options
        .getKmsKeyName()
        .ifPresent(
            destinationTableKmsKeyName ->
                jobConfiguration.setDestinationEncryptionConfiguration(
                    EncryptionConfiguration.newBuilder()
                        .setKmsKeyName(destinationTableKmsKeyName)
                        .build()));

    Job finishedJob = null;
    try {
      finishedJob = createAndWaitFor(jobConfiguration);

      if (finishedJob.getStatus().getError() != null) {
        throw new BigQueryException(
            BaseHttpServiceException.UNKNOWN_CODE,
            String.format(
                "Failed to load to %s in job %s. BigQuery error was '%s'",
                BigQueryUtil.friendlyTableName(options.getTableId()),
                finishedJob.getJobId(),
                finishedJob.getStatus().getError().getMessage()),
            finishedJob.getStatus().getError());
      } else {
        log.info(
            "Done loading to {}. jobId: {}",
            BigQueryUtil.friendlyTableName(options.getTableId()),
            finishedJob.getJobId());
      }
    } catch (Exception e) {
      if (finishedJob == null) {
        log.error(
            "Unable to create the job to load to {}",
            BigQueryUtil.friendlyTableName(options.getTableId()));
        throw e;
      }
      TimePartitioning.Type partitionType = options.getPartitionTypeOrDefault();

      if (e.getMessage()
              .equals(
                  String.format("Cannot output %s partitioned data in LegacySQL", partitionType))
          && formatOptions.equals(FormatOptions.parquet())) {
        throw new BigQueryException(
            0,
            String.format(
                "%s time partitioning is not available "
                    + "for load jobs from PARQUET in this project yet. Please replace the intermediate "
                    + "format to AVRO or contact your account manager to enable this.",
                partitionType),
            e);
      }
      JobId jobId = finishedJob.getJobId();
      log.warn(
          String.format(
              "Failed to load the data into BigQuery, JobId for debug purposes is [%s:%s.%s]",
              jobId.getProject(), jobId.getLocation(), jobId.getJob()));
      throw new BigQueryException(0, "Problem loading data into BigQuery", e);
    }
  }

  /** Creates the table with the given schema, only if it does not exist yet. */
  public void createTableIfNeeded(
      TableId tableId, Schema bigQuerySchema, CreateTableOptions options) {
    if (!tableExists(tableId)) {
      createTable(tableId, bigQuerySchema, options);
    }
  }

  public interface ReadTableOptions {
    TableId tableId();

    Optional<String> query();

    boolean viewsEnabled();

    String viewEnabledParamName();

    int expirationTimeInMinutes();
  }

  public interface LoadDataOptions {
    TableId getTableId();

    Optional<JobInfo.CreateDisposition> getCreateDisposition();

    Optional<String> getPartitionField();

    Optional<TimePartitioning.Type> getPartitionType();

    Optional<RangePartitioning.Range> getPartitionRange();

    TimePartitioning.Type getPartitionTypeOrDefault();

    OptionalLong getPartitionExpirationMs();

    Optional<Boolean> getPartitionRequireFilter();

    Optional<ImmutableList<String>> getClusteredFields();

    boolean isUseAvroLogicalTypes();

    List<String> getDecimalTargetTypes();

    List<JobInfo.SchemaUpdateOption> getLoadSchemaUpdateOptions();

    boolean getEnableModeCheckForSchemaFields();

    Optional<String> getKmsKeyName();
  }

  public interface CreateTableOptions {

    default Optional<String> getKmsKeyName() {
      return Optional.empty();
    }

    default Map<String, String> getBigQueryTableLabels() {
      return Collections.emptyMap();
    }

    static CreateTableOptions of(
        final Optional<String> kmsKeyName, final Map<String, String> bigQueryTableLabels) {
      return new CreateTableOptions() {
        @Override
        public Optional<String> getKmsKeyName() {
          return kmsKeyName;
        }

        @Override
        public Map<String, String> getBigQueryTableLabels() {
          return bigQueryTableLabels;
        }
      };
    }
  }

  static class DestinationTableBuilder implements Callable<TableInfo> {
    final BigQueryClient bigQueryClient;
    final String querySql;
    final TableId destinationTable;
    final int expirationTimeInMinutes;
    final JobConfigurationFactory jobConfigurationFactory;
    final Map<String, String> additionalQueryJobLabels;

    DestinationTableBuilder(
        BigQueryClient bigQueryClient,
        String querySql,
        TableId destinationTable,
        int expirationTimeInMinutes,
        JobConfigurationFactory jobConfigurationFactory,
        Map<String, String> additionalQueryJobLabels) {
      this.bigQueryClient = bigQueryClient;
      this.querySql = querySql;
      this.destinationTable = destinationTable;
      this.expirationTimeInMinutes = expirationTimeInMinutes;
      this.jobConfigurationFactory = jobConfigurationFactory;
      this.additionalQueryJobLabels = additionalQueryJobLabels;
    }

    @Override
    public TableInfo call() {
      return createTableFromQuery();
    }

    TableInfo createTableFromQuery() {
      log.debug("destinationTable is {}", destinationTable);
      JobInfo jobInfo =
          JobInfo.of(
              jobConfigurationFactory
                  .createQueryJobConfigurationBuilder(querySql, additionalQueryJobLabels)
                  .setDestinationTable(destinationTable)
                  .build());

      log.debug("running query {}", jobInfo);
      Job job = waitForJob(bigQueryClient.create(jobInfo));
      log.debug("job has finished. {}", job);
      if (job.getStatus().getError() != null) {
        throw BigQueryUtil.convertToBigQueryException(job.getStatus().getError());
      }
      // add expiration time to the table
      TableInfo createdTable = bigQueryClient.getTable(destinationTable);
      long expirationTime =
          createdTable.getCreationTime() + TimeUnit.MINUTES.toMillis(expirationTimeInMinutes);
      Table updatedTable =
          bigQueryClient.update(createdTable.toBuilder().setExpirationTime(expirationTime).build());
      return updatedTable;
    }

    Job waitForJob(Job job) {
      try {
        return job.waitFor();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new BigQueryException(
            BaseServiceException.UNKNOWN_CODE,
            String.format("Job %s has been interrupted", job.getJobId()),
            e);
      }
    }
  }

  static class JobConfigurationFactory {
    private final ImmutableMap<String, String> labels;
    private final Priority queryJobPriority;

    public JobConfigurationFactory(Map<String, String> labels, Priority queryJobPriority) {
      this.labels = ImmutableMap.copyOf(labels);
      this.queryJobPriority = queryJobPriority;
    }

    QueryJobConfiguration.Builder createQueryJobConfigurationBuilder(
        String querySql, Map<String, String> additionalQueryJobLabels) {
      QueryJobConfiguration.Builder builder =
          QueryJobConfiguration.newBuilder(querySql).setPriority(queryJobPriority);
      Map<String, String> allLabels = new HashMap<>(additionalQueryJobLabels);

      if (labels != null && !labels.isEmpty()) {
        allLabels.putAll(labels);
      }

      builder.setLabels(allLabels);
      return builder;
    }

    LoadJobConfiguration.Builder createLoadJobConfigurationBuilder(
        LoadDataOptions options, List<String> sourceUris, FormatOptions formatOptions) {
      LoadJobConfiguration.Builder builder =
          LoadJobConfiguration.newBuilder(options.getTableId(), sourceUris, formatOptions);
      if (labels != null && !labels.isEmpty()) {
        builder.setLabels(labels);
      }
      return builder;
    }
  }
}
