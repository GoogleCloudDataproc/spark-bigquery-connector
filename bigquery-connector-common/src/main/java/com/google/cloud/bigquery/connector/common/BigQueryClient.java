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

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.getPartitionFields;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.getQueryForRangePartitionedTable;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.getQueryForTimePartitionedTable;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.isBigQueryNativeTable;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.BaseServiceException;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
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
import java.security.GeneralSecurityException;
import java.util.ArrayList;
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

  private static final List<Runnable> CLEANUP_JOBS = new ArrayList<>();

  private final BigQuery bigQuery;
  // The rest client is generated directly from the API, and therefore returns more metadata than
  // the
  // google-cloud-bigquery client above,
  private final Bigquery bigqueryRestClient;
  private final Cache<String, TableInfo> destinationTableCache;
  private final Optional<String> materializationProject;
  private final Optional<String> materializationDataset;
  private final JobConfigurationFactory jobConfigurationFactory;
  private final Optional<BigQueryJobCompletionListener> jobCompletionListener;
  private final long bigQueryJobTimeoutInMinutes;

  public BigQueryClient(
      BigQuery bigQuery,
      Optional<String> materializationProject,
      Optional<String> materializationDataset,
      Cache<String, TableInfo> destinationTableCache,
      Map<String, String> labels,
      Priority queryJobPriority,
      Optional<BigQueryJobCompletionListener> jobCompletionListener,
      long bigQueryJobTimeoutInMinutes) {
    this.bigQuery = bigQuery;
    this.bigqueryRestClient = createRestClient(bigQuery);
    this.materializationProject = materializationProject;
    this.materializationDataset = materializationDataset;
    this.destinationTableCache = destinationTableCache;
    this.jobConfigurationFactory = new JobConfigurationFactory(labels, queryJobPriority);
    this.jobCompletionListener = jobCompletionListener;
    this.bigQueryJobTimeoutInMinutes = bigQueryJobTimeoutInMinutes;
  }

  static Bigquery createRestClient(BigQuery bigQuery) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests
      HttpCredentialsAdapter httpCredentialsAdapter =
          new HttpCredentialsAdapter(bigQuery.getOptions().getCredentials());
      Bigquery.Builder client =
          new Bigquery.Builder(
                  GoogleNetHttpTransport.newTrustedTransport(),
                  GsonFactory.getDefaultInstance(),
                  httpRequest -> {
                    httpCredentialsAdapter.initialize(httpRequest);
                    httpRequest.setThrowExceptionOnExecuteError(false);
                  })
              .setApplicationName(bigQuery.getOptions().getUserAgent());
      return client.build();
    } catch (GeneralSecurityException gse) {
      throw new BigQueryConnectorException(
          "Failed to create new trusted transport for BigQuery REST client", gse);
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  public static synchronized void runCleanupJobs() {
    log.info("Running cleanup jobs. Jobs count is " + CLEANUP_JOBS.size());
    for (Runnable job : CLEANUP_JOBS) {
      try {
        job.run();
      } catch (Exception e) {
        log.warn(
            "Caught exception while running cleanup job. Continue to run the rest of the jobs", e);
      }
    }
    log.info("Clearing the cleanup jobs list");
    CLEANUP_JOBS.clear();
    log.info("Finished to run cleanup jobs.");
  }

  /**
   * Waits for a BigQuery Job to complete: this is a blocking function.
   *
   * @param job The {@code Job} to keep track of.
   */
  public JobInfo waitForJob(Job job) {
    try {
      Job completedJob =
          job.waitFor(
              RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
              RetryOption.totalTimeout(Duration.ofMinutes(bigQueryJobTimeoutInMinutes)));
      if (completedJob == null || completedJob.getStatus().getError() != null) {
        throw new UncheckedIOException(
            new IOException(
                completedJob != null ? completedJob.getStatus().getError().toString() : null));
      }
      if (!completedJob.isDone()) {
        completedJob.cancel();
        throw new IllegalStateException(
            String.format("Job aborted due to timeout  : %s minutes", bigQueryJobTimeoutInMinutes));
      }
      jobCompletionListener.ifPresent(jcl -> jcl.accept(completedJob));
      return completedJob;
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

    StandardTableDefinition.Builder tableDefinition =
        StandardTableDefinition.newBuilder().setSchema(schema);

    options
        .getClusteredFields()
        .ifPresent(
            clusteredFields ->
                tableDefinition.setClustering(
                    Clustering.newBuilder().setFields(clusteredFields).build()));

    TableInfo.Builder tableInfo = TableInfo.newBuilder(tableId, tableDefinition.build());
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
   * Creates a temporary table with a job to cleanup after application end, and the same location as
   * the destination table; the temporary table will have the same name as the destination table,
   * with the current time in milliseconds appended to it; useful for holding temporary data in
   * order to overwrite the destination table.
   *
   * @param destinationTableId The TableId of the eventual destination for the data going into the
   *     temporary table.
   * @param schema The Schema of the destination / temporary table.
   * @return The {@code Table} object representing the created temporary table.
   */
  public TableInfo createTempTable(TableId destinationTableId, Schema schema) {
    TableId tempTableId = createTempTableId(destinationTableId);
    TableInfo tableInfo =
        TableInfo.newBuilder(tempTableId, StandardTableDefinition.of(schema)).build();
    TableInfo tempTable = bigQuery.create(tableInfo);
    CLEANUP_JOBS.add(() -> deleteTable(tempTable.getTableId()));
    return tempTable;
  }

  public TableInfo createTempTableAfterCheckingSchema(
      TableId destinationTableId, Schema schema, boolean enableModeCheckForSchemaFields)
      throws IllegalArgumentException {
    TableInfo destinationTable = getTable(destinationTableId);
    Schema tableSchema = destinationTable.getDefinition().getSchema();
    ComparisonResult schemaWritableResult =
        BigQueryUtil.schemaWritable(
            schema, // sourceSchema
            tableSchema, // destinationSchema
            false, // regardFieldOrder
            enableModeCheckForSchemaFields);
    Preconditions.checkArgument(
        schemaWritableResult.valuesAreEqual(),
        new BigQueryConnectorException.InvalidSchemaException(
            "Destination table's schema is not compatible with dataframe's schema. "
                + schemaWritableResult.makeMessage()));
    return createTempTable(destinationTableId, schema);
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
    log.info("Deleting table " + fullTableName(tableId));
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

  public boolean isTablePartitioned(TableId tableId) {
    TableInfo table = getTable(tableId);
    if (table == null) {
      return false;
    }
    TableDefinition tableDefinition = table.getDefinition();
    if (tableDefinition instanceof StandardTableDefinition) {
      StandardTableDefinition sdt = (StandardTableDefinition) tableDefinition;
      return sdt.getTimePartitioning() != null || sdt.getRangePartitioning() != null;
    }
    return false;
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
      } else {
        RangePartitioning rangePartitioning = sdt.getRangePartitioning();
        if (rangePartitioning != null) {
          sqlQuery =
              getQueryForRangePartitionedTable(
                  destinationTableName, temporaryTableName, sdt, rangePartitioning);
        }
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
      return materializeQueryToTable(
          sql,
          options.expirationTimeInMinutes(),
          options.getQueryParameterHelper(),
          options.getKmsKeyName());
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

  /**
   * Returns the schema of the table/query/view. Uses dryRun to get the query schema instead of
   * materializing it.
   *
   * @param options The {@code ReadTableOptions} options for reading the data source.
   * @return The schema.
   */
  public Schema getReadTableSchema(ReadTableOptions options) {
    Optional<String> query = options.query();
    // lazy materialization if it's a query
    if (query.isPresent()) {
      validateViewsEnabled(options);
      String sql = query.get();
      return getQueryResultSchema(
          sql, Collections.emptyMap(), options.getQueryParameterHelper(), options.getKmsKeyName());
    }
    TableInfo table = getReadTable(options);
    return table != null ? table.getDefinition().getSchema() : null;
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

  public Iterable<Dataset> listDatasets() {
    return listDatasetsForProject(getProjectId());
  }

  public Iterable<Dataset> listDatasetsForProject(String projectId) {
    return bigQuery.listDatasets(projectId).iterateAll();
  }

  public Iterable<Table> listTables(DatasetId datasetId, TableDefinition.Type... types) {
    Set<TableDefinition.Type> allowedTypes = ImmutableSet.copyOf(types);
    Iterable<Table> allTables = bigQuery.listTables(datasetId).iterateAll();
    return StreamSupport.stream(allTables.spliterator(), false)
        .filter(table -> allowedTypes.contains(table.getDefinition().getType()))
        .collect(ImmutableList.toImmutableList());
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

  String createSql(
      TableId table,
      ImmutableList<String> requiredColumns,
      String[] filters,
      OptionalLong snapshotTimeMillis) {
    String columns =
        requiredColumns.isEmpty()
            ? "*"
            : requiredColumns.stream()
                .map(column -> String.format("`%s`", column))
                .collect(Collectors.joining(","));

    return createSql(table, columns, filters, snapshotTimeMillis);
  }

  // assuming the SELECT part is properly formatted, can be used to call functions such as COUNT and
  // SUM
  String createSql(
      TableId table, String formattedQuery, String[] filters, OptionalLong snapshotTimeMillis) {
    String tableName = fullTableName(table);

    String whereClause = createWhereClause(filters).map(clause -> "WHERE " + clause).orElse("");

    String snapshotTimeClause =
        snapshotTimeMillis.isPresent()
            ? String.format(
                "FOR SYSTEM_TIME AS OF TIMESTAMP_MILLIS(%d)", snapshotTimeMillis.getAsLong())
            : "";

    return String.format(
        "SELECT %s FROM `%s` %s %s", formattedQuery, tableName, whereClause, snapshotTimeClause);
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
    if ((type == TableDefinition.Type.EXTERNAL || type == TableDefinition.Type.TABLE)
        && !filter.isPresent()) {
      if (isBigQueryNativeTable(tableInfo)
          && tableInfo.getRequirePartitionFilter() != null
          && tableInfo.getRequirePartitionFilter()) {
        List<String> partitioningFields = getPartitionFields(tableInfo);
        if (partitioningFields.isEmpty()) {
          throw new IllegalStateException(
              "Could not find partitioning columns for table requiring partition filter: "
                  + tableInfo.getTableId());
        }
        String table = fullTableName(tableInfo.getTableId());
        return getNumberOfRows(
            String.format(
                "SELECT COUNT(*) from `%s` WHERE %s IS NOT NULL",
                table, partitioningFields.get(0)));
      }
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
   * @param expirationTimeInMinutes the expiration time of the temporary table
   * @param queryParameterHelper the query parameter helper
   * @param kmsKeyName optional KMS key name to be used for encrypting the temporary table
   * @return a reference to the table
   */
  public TableInfo materializeQueryToTable(
      String querySql,
      int expirationTimeInMinutes,
      QueryParameterHelper queryParameterHelper,
      Optional<String> kmsKeyName) {
    Optional<TableId> tableId =
        materializationDataset.map(ignored -> createDestinationTableWithoutReference());
    return materializeTable(
        querySql, tableId, expirationTimeInMinutes, queryParameterHelper, kmsKeyName);
  }

  TableId createDestinationTableWithoutReference() {
    return createDestinationTable(Optional.empty(), Optional.empty());
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

  /**
   * Runs the provided query on BigQuery and saves the result in a temporary table.
   *
   * @param querySql the query to be run
   * @param additionalQueryJobLabels the labels to insert on the query job
   * @return a reference to the table
   */
  public TableInfo materializeQueryToTable(
      String querySql,
      int expirationTimeInMinutes,
      Map<String, String> additionalQueryJobLabels,
      QueryParameterHelper queryParameterHelper) {
    return materializeQueryToTable(
        querySql,
        expirationTimeInMinutes,
        additionalQueryJobLabels,
        queryParameterHelper,
        Optional.empty());
  }

  /**
   * Runs the provided query on BigQuery and saves the result in a temporary table.
   *
   * @param querySql the query to be run
   * @param additionalQueryJobLabels the labels to insert on the query job
   * @return a reference to the table
   */
  public TableInfo materializeQueryToTable(
      String querySql,
      int expirationTimeInMinutes,
      Map<String, String> additionalQueryJobLabels,
      QueryParameterHelper queryParameterHelper,
      Optional<String> kmsKeyName) {
    Optional<TableId> destinationTableId =
        materializationDataset.map(ignored -> createDestinationTableWithoutReference());
    TempTableBuilder tableBuilder =
        new TempTableBuilder(
            this,
            querySql,
            destinationTableId,
            expirationTimeInMinutes,
            jobConfigurationFactory,
            additionalQueryJobLabels,
            queryParameterHelper,
            kmsKeyName);

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
   * @return a reference to the table
   */
  public TableInfo materializeViewToTable(
      String querySql, TableId viewId, int expirationTimeInMinutes) {
    TableId tableId =
        createDestinationTable(
            Optional.ofNullable(viewId.getProject()), Optional.ofNullable(viewId.getDataset()));
    return materializeTable(
        querySql,
        Optional.of(tableId),
        expirationTimeInMinutes,
        QueryParameterHelper.none(),
        Optional.empty());
  }

  public Schema getQueryResultSchema(
      String querySql,
      Map<String, String> additionalQueryJobLabels,
      QueryParameterHelper queryParameterHelper) {
    return getQueryResultSchema(
        querySql, additionalQueryJobLabels, queryParameterHelper, Optional.empty());
  }

  public Schema getQueryResultSchema(
      String querySql,
      Map<String, String> additionalQueryJobLabels,
      QueryParameterHelper queryParameterHelper,
      Optional<String> kmsKeyName) {
    QueryJobConfiguration.Builder builder =
        jobConfigurationFactory
            .createParameterizedQueryJobConfigurationBuilder(
                querySql, additionalQueryJobLabels, queryParameterHelper)
            .setDryRun(true);
    kmsKeyName.ifPresent(
        k ->
            builder.setDestinationEncryptionConfiguration(
                EncryptionConfiguration.newBuilder().setKmsKeyName(k).build()));
    JobInfo jobInfo = JobInfo.of(builder.build());

    log.info("running query dryRun {}", querySql);
    JobInfo completedJobInfo = create(jobInfo);
    if (completedJobInfo.getStatus().getError() != null) {
      throw BigQueryUtil.convertToBigQueryException(completedJobInfo.getStatus().getError());
    }
    JobStatistics.QueryStatistics queryStatistics = completedJobInfo.getStatistics();
    return queryStatistics.getSchema();
  }

  private TableInfo materializeTable(
      String querySql,
      Optional<TableId> destinationTableId,
      int expirationTimeInMinutes,
      QueryParameterHelper queryParameterHelper) {
    return materializeTable(
        querySql,
        destinationTableId,
        expirationTimeInMinutes,
        queryParameterHelper,
        Optional.empty());
  }

  private TableInfo materializeTable(
      String querySql,
      Optional<TableId> destinationTableId,
      int expirationTimeInMinutes,
      QueryParameterHelper queryParameterHelper,
      Optional<String> kmsKeyName) {
    try {
      return destinationTableCache.get(
          querySql,
          new TempTableBuilder(
              this,
              querySql,
              destinationTableId,
              expirationTimeInMinutes,
              jobConfigurationFactory,
              Collections.emptyMap(),
              queryParameterHelper,
              kmsKeyName));
    } catch (Exception e) {
      throw new BigQueryConnectorException(
          BigQueryErrorCode.BIGQUERY_VIEW_DESTINATION_TABLE_CREATION_FAILED,
          String.format(
              "Error creating destination table using the following query: [%s]", querySql),
          e);
    }
  }

  private TableInfo materializeTable(String querySql, TempTableBuilder tmpTableBuilder) {
    try {
      return destinationTableCache.get(querySql, tmpTableBuilder);
    } catch (Exception e) {
      throw new BigQueryConnectorException(
          BigQueryErrorCode.BIGQUERY_VIEW_DESTINATION_TABLE_CREATION_FAILED,
          String.format(
              "Error creating destination table using the following query: [%s]", querySql),
          e);
    }
  }

  public JobStatistics.LoadStatistics loadDataIntoTable(
      LoadDataOptions options,
      List<String> sourceUris,
      FormatOptions formatOptions,
      JobInfo.WriteDisposition writeDisposition,
      Optional<Schema> schema,
      TableId destinationTable) {
    LoadJobConfiguration.Builder jobConfiguration =
        jobConfigurationFactory
            .createLoadJobConfigurationBuilder(destinationTable, sourceUris, formatOptions)
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
        return finishedJob.getStatistics();
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

  /**
   * Retrieves the table's metadata from the REST client, may contain more information than the
   * regular one
   */
  public Optional<com.google.api.services.bigquery.model.Table> getRestTable(TableId tableId) {
    try {
      // tableId.getProject() may be null, so we replacing it with the default project id
      String project =
          Optional.ofNullable(tableId.getProject())
              .orElseGet(() -> bigQuery.getOptions().getProjectId());
      return Optional.ofNullable(
          bigqueryRestClient
              .tables()
              .get(project, tableId.getDataset(), tableId.getTable())
              .execute());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Returns true f the dataset exists, false otherwise. */
  public boolean datasetExists(DatasetId datasetId) {
    return bigQuery.getDataset(datasetId) != null;
  }

  public void createDataset(DatasetId datasetId, Map<String, String> metadata) {
    DatasetInfo.Builder datasetInfo = DatasetInfo.newBuilder(datasetId);
    // In a non-catalog scenario, both BigQueryOptions.quotaProjectId and
    // BigQueryOptions.projectId
    // will be set to the parent projectId, as before.
    Optional.ofNullable(bigQuery.getOptions().getLocation()).ifPresent(datasetInfo::setLocation);
    if (metadata != null && !metadata.isEmpty()) {
      Optional.ofNullable(metadata.get("bigquery_location")).ifPresent(datasetInfo::setLocation);
      Optional.ofNullable(metadata.get("comment")).ifPresent(datasetInfo::setDescription);
    }
    bigQuery.create(datasetInfo.build());
  }

  public boolean deleteDataset(DatasetId datasetId, boolean cascade) {
    BigQuery.DatasetDeleteOption[] options =
        cascade
            ? new BigQuery.DatasetDeleteOption[] {BigQuery.DatasetDeleteOption.deleteContents()}
            : new BigQuery.DatasetDeleteOption[] {};
    return bigQuery.delete(datasetId, options);
  }

  public DatasetInfo getDataset(DatasetId datasetId) {
    return bigQuery.getDataset(datasetId);
  }

  public interface ReadTableOptions {
    TableId tableId();

    Optional<String> query();

    boolean viewsEnabled();

    String viewEnabledParamName();

    int expirationTimeInMinutes();

    QueryParameterHelper getQueryParameterHelper();

    default Optional<String> getKmsKeyName() {
      return Optional.empty();
    }
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

    default Optional<ImmutableList<String>> getClusteredFields() {
      return Optional.empty();
    }

    static CreateTableOptions of(
        final Optional<String> kmsKeyName,
        final Map<String, String> bigQueryTableLabels,
        final Optional<ImmutableList<String>> clusteredFields) {
      return new CreateTableOptions() {
        @Override
        public Optional<String> getKmsKeyName() {
          return kmsKeyName;
        }

        @Override
        public Map<String, String> getBigQueryTableLabels() {
          return bigQueryTableLabels;
        }

        @Override
        public Optional<ImmutableList<String>> getClusteredFields() {
          return clusteredFields;
        }
      };
    }
  }

  static class TempTableBuilder implements Callable<TableInfo> {
    final BigQueryClient bigQueryClient;
    final String querySql;
    final Optional<TableId> tempTable;
    final int expirationTimeInMinutes;
    final JobConfigurationFactory jobConfigurationFactory;
    final Map<String, String> additionalQueryJobLabels;
    final QueryParameterHelper queryParameterHelper;
    final Optional<String> kmsKeyName;

    TempTableBuilder(
        BigQueryClient bigQueryClient,
        String querySql,
        Optional<TableId> tempTable,
        int expirationTimeInMinutes,
        JobConfigurationFactory jobConfigurationFactory,
        Map<String, String> additionalQueryJobLabels,
        QueryParameterHelper queryParameterHelper) {
      this(
          bigQueryClient,
          querySql,
          tempTable,
          expirationTimeInMinutes,
          jobConfigurationFactory,
          additionalQueryJobLabels,
          queryParameterHelper,
          Optional.empty());
    }

    TempTableBuilder(
        BigQueryClient bigQueryClient,
        String querySql,
        Optional<TableId> tempTable,
        int expirationTimeInMinutes,
        JobConfigurationFactory jobConfigurationFactory,
        Map<String, String> additionalQueryJobLabels,
        QueryParameterHelper queryParameterHelper,
        Optional<String> kmsKeyName) {
      this.bigQueryClient = bigQueryClient;
      this.querySql = querySql;
      this.tempTable = tempTable;
      this.expirationTimeInMinutes = expirationTimeInMinutes;
      this.jobConfigurationFactory = jobConfigurationFactory;
      this.additionalQueryJobLabels = additionalQueryJobLabels;
      this.queryParameterHelper = queryParameterHelper;
      this.kmsKeyName = kmsKeyName;
    }

    @Override
    public TableInfo call() {
      return createTableFromQuery();
    }

    TableInfo createTableFromQuery() {
      if (tempTable.isPresent()) {
        log.info("DestinationTable is {}", tempTable.get());
      } else {
        log.info("DestinationTable is automatically generated");
      }
      QueryJobConfiguration.Builder queryJobConfigurationBuilder =
          jobConfigurationFactory.createParameterizedQueryJobConfigurationBuilder(
              querySql, additionalQueryJobLabels, queryParameterHelper);
      tempTable.ifPresent(queryJobConfigurationBuilder::setDestinationTable);
      kmsKeyName.ifPresent(
          k ->
              queryJobConfigurationBuilder.setDestinationEncryptionConfiguration(
                  EncryptionConfiguration.newBuilder().setKmsKeyName(k).build()));

      JobInfo jobInfo = JobInfo.of(queryJobConfigurationBuilder.build());

      log.info("running query [{}]", querySql);
      JobInfo completedJobInfo = bigQueryClient.waitForJob(bigQueryClient.create(jobInfo));
      if (completedJobInfo.getStatus().getError() != null) {
        throw BigQueryUtil.convertToBigQueryException(completedJobInfo.getStatus().getError());
      }
      if (tempTable.isPresent()) {
        // Registering a cleanup job
        CLEANUP_JOBS.add(() -> bigQueryClient.deleteTable(tempTable.get()));
        // add expiration time to the table
        TableInfo createdTable = bigQueryClient.getTable(tempTable.get());
        long expirationTime =
            createdTable.getCreationTime() + TimeUnit.MINUTES.toMillis(expirationTimeInMinutes);
        Table updatedTable =
            bigQueryClient.update(
                createdTable.toBuilder().setExpirationTime(expirationTime).build());
        return updatedTable;
      }
      // temp table was auto generated
      return bigQueryClient.getTable(
          ((QueryJobConfiguration) completedJobInfo.getConfiguration()).getDestinationTable());
    }

    Job waitForJob(Job job) {
      try {
        log.info(
            "Job submitted : {}, {},  Job type : {}",
            job.getJobId(),
            job.getSelfLink(),
            job.getConfiguration().getType());
        Job completedJob = job.waitFor();
        log.info(
            "Job has finished {} creationTime : {}, startTime : {}, endTime : {} ",
            completedJob.getJobId(),
            completedJob.getStatistics().getCreationTime(),
            completedJob.getStatistics().getStartTime(),
            completedJob.getStatistics().getEndTime());
        log.debug("Job has finished. {}", completedJob);
        return completedJob;
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
      return createParameterizedQueryJobConfigurationBuilder(
          querySql, additionalQueryJobLabels, QueryParameterHelper.none());
    }

    QueryJobConfiguration.Builder createParameterizedQueryJobConfigurationBuilder(
        String querySql,
        Map<String, String> additionalQueryJobLabels,
        QueryParameterHelper queryParameterHelper) {

      QueryJobConfiguration.Builder builder =
          QueryJobConfiguration.newBuilder(querySql).setPriority(queryJobPriority);

      queryParameterHelper.configureBuilder(builder);

      Map<String, String> allLabels = new HashMap<>(additionalQueryJobLabels);

      if (labels != null && !labels.isEmpty()) {
        allLabels.putAll(labels);
      }

      builder.setLabels(allLabels);
      return builder;
    }

    LoadJobConfiguration.Builder createLoadJobConfigurationBuilder(
        TableId tableId, List<String> sourceUris, FormatOptions formatOptions) {
      LoadJobConfiguration.Builder builder =
          LoadJobConfiguration.newBuilder(tableId, sourceUris, formatOptions);
      if (labels != null && !labels.isEmpty()) {
        builder.setLabels(labels);
      }
      return builder;
    }
  }
}
