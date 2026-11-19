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
package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.api.core.NanoClock;
import com.google.api.gax.retrying.DirectRetryingExecutor;
import com.google.api.gax.retrying.ExponentialRetryAlgorithm;
import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.api.gax.retrying.RetryAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.retrying.RetryingFuture;
import com.google.api.gax.retrying.TimedAttemptSettings;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTestUtils {

  static Logger logger = LoggerFactory.getLogger(IntegrationTestUtils.class);

  private static Cache<String, TableInfo> destinationTableCache =
      CacheBuilder.newBuilder().expireAfterWrite(15, TimeUnit.MINUTES).maximumSize(1000).build();

  private static final ImmutableSet<String> TRANSIENT_BIGQUERY_REASONS =
      ImmutableSet.of("backendError", "internalError", "rateLimitExceeded");

  private static final RetrySettings BIGQUERY_RETRY_SETTINGS =
      RetrySettings.newBuilder()
          .setMaxAttempts(5)
          .setInitialRetryDelayDuration(Duration.ofSeconds(1))
          .setRetryDelayMultiplier(2.0)
          .setMaxRetryDelayDuration(Duration.ofSeconds(16))
          .setTotalTimeoutDuration(Duration.ofMinutes(2))
          .build();

  // google-cloud-bigquery invokes HttpBigQueryRpc.*SkipExceptionTranslation inside its retry loop,
  // which throws raw GoogleJsonResponseException (not BaseServiceException) so the default
  // exception handler does not retry HTTP 5xx responses such as 503 backendError. Query jobs can
  // also finish with transient error reasons (code 0, isRetryable() == false). Wrapping test
  // fixture operations here retries both cases with bounded exponential backoff.
  static boolean isTransientBigQueryError(Throwable t) {
    if (!(t instanceof BigQueryException)) {
      return false;
    }
    BigQueryException bqException = (BigQueryException) t;
    if (bqException.isRetryable() || bqException.getCode() == 429) {
      return true;
    }
    if (bqException.getReason() != null
        && TRANSIENT_BIGQUERY_REASONS.contains(bqException.getReason())) {
      return true;
    }
    BigQueryError error = bqException.getError();
    return error != null
        && error.getReason() != null
        && TRANSIENT_BIGQUERY_REASONS.contains(error.getReason());
  }

  static <T> T runWithRetries(Callable<T> action, String operationName) {
    return runWithRetries(action, BIGQUERY_RETRY_SETTINGS, operationName);
  }

  static <T> T runWithRetries(
      Callable<T> action, RetrySettings retrySettings, String operationName) {
    AtomicInteger attempt = new AtomicInteger(0);
    ResultRetryAlgorithm<T> resultRetryAlgorithm =
        new ResultRetryAlgorithm<T>() {
          @Override
          public TimedAttemptSettings createNextAttempt(
              Throwable prevThrowable, T prevResponse, TimedAttemptSettings prevSettings) {
            return null;
          }

          @Override
          public boolean shouldRetry(Throwable prevThrowable, T prevResponse) {
            return isTransientBigQueryError(prevThrowable);
          }
        };
    DirectRetryingExecutor<T> executor =
        new DirectRetryingExecutor<>(
            new RetryAlgorithm<>(
                resultRetryAlgorithm,
                new ExponentialRetryAlgorithm(retrySettings, NanoClock.getDefaultClock())));
    RetryingFuture<T> future =
        executor.createFuture(
            () -> {
              int currentAttempt = attempt.incrementAndGet();
              try {
                return action.call();
              } catch (Exception e) {
                if (isTransientBigQueryError(e)
                    && currentAttempt < retrySettings.getMaxAttempts()) {
                  logger.warn(
                      "Transient BigQuery error during {} (attempt {}/{}), retrying: {}",
                      operationName,
                      currentAttempt,
                      retrySettings.getMaxAttempts(),
                      e.getMessage());
                }
                throw e;
              }
            });
    try {
      return executor.submit(future).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during " + operationName, e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException("Failed during " + operationName, cause);
    }
  }

  public static BigQuery getBigquery() {
    return getBigquery(Optional.empty());
  }

  public static BigQuery getBigquery(Optional<String> location) {
    BigQueryOptions.Builder options = BigQueryOptions.newBuilder();
    location.ifPresent(options::setLocation);
    Optional.ofNullable(System.getenv("BIGQUERY_API_HTTP_ENDPOINT")).ifPresent(options::setHost);
    return options.build().getService();
  }

  private static BigQueryClient getBigQueryClient() {
    return getBigQueryClient(Optional.empty());
  }

  private static BigQueryClient getBigQueryClient(Optional<String> location) {
    return new BigQueryClient(
        getBigquery(location),
        Optional.empty(),
        Optional.empty(),
        destinationTableCache,
        ImmutableMap.of(),
        SparkBigQueryConfig.DEFAULT_JOB_PRIORITY,
        Optional.empty(),
        6 * 60);
  }

  public static void createDataset(String dataset) {
    BigQuery bq = getBigquery();
    DatasetId datasetId = DatasetId.of(dataset);
    logger.warn("Creating test dataset: {}", datasetId);
    runWithRetries(() -> bq.create(DatasetInfo.of(datasetId)), "createDataset(" + dataset + ")");
  }

  public static TableResult runQuery(String query) {
    BigQueryClient client = getBigQueryClient();
    return runWithRetries(() -> client.query(query), "runQuery");
  }

  public static void runQuery(String query, Object... args) {
    runQuery(String.format(query, args));
  }

  public static void runQueryInLocation(String bigQueryLocation, String query, Object... args) {
    BigQuery bq = getBigquery(Optional.ofNullable(bigQueryLocation));
    String formattedQuery = String.format(query, args);
    runWithRetries(
        () ->
            bq.create(
                    JobInfo.newBuilder(QueryJobConfiguration.newBuilder(formattedQuery).build())
                        .setJobId(
                            JobId.newBuilder()
                                .setJob(
                                    UUID.randomUUID()
                                        .toString()) // Generates a unique ID for the job
                                .setLocation(bigQueryLocation)
                                .build())
                        .build())
                .waitFor(),
        "runQueryInLocation(" + bigQueryLocation + ")");
  }

  public static Iterable<Table> listTables(DatasetId datasetId, TableDefinition.Type... types) {
    BigQueryClient client = getBigQueryClient();
    return runWithRetries(
        () -> client.listTables(datasetId, types), "listTables(" + datasetId.getDataset() + ")");
  }

  public static void createBigLakeTable(
      String dataset,
      String table,
      StructType schema,
      String sourceURI,
      FormatOptions formatOptions) {
    BigQuery bq = getBigquery();
    TableId tableId = TableId.of(dataset, table);
    TableInfo tableInfo =
        TableInfo.newBuilder(
                tableId,
                ExternalTableDefinition.newBuilder(
                        sourceURI,
                        SchemaConverters.from(SchemaConvertersConfiguration.createDefault())
                            .toBigQuerySchema(schema),
                        formatOptions)
                    .setConnectionId(TestConstants.BIGLAKE_CONNECTION_ID)
                    .build())
            .build();
    runWithRetries(() -> bq.create(tableInfo), "createBigLakeTable(" + dataset + "." + table + ")");
  }

  public static void deleteDatasetAndTables(String dataset) {
    BigQuery bq = getBigquery();
    logger.warn("Deleting test dataset '{}' and its contents", dataset);
    runWithRetries(
        () -> bq.delete(DatasetId.of(dataset), BigQuery.DatasetDeleteOption.deleteContents()),
        "deleteDatasetAndTables(" + dataset + ")");
  }

  static Metadata metadata(String key, String value) {
    MetadataBuilder metadata = new MetadataBuilder();
    metadata.putString(key, value);
    return metadata.build();
  }

  static SparkSession getOrCreateSparkSession(String applicationName) {
    return SparkSession.builder()
        .appName(applicationName)
        .master(System.getProperty("spark.master", "local"))
        .getOrCreate();
  }

  static void createView(String dataset, String view) {
    BigQuery bq = getBigquery();
    String query = "SELECT * FROM `bigquery-public-data.samples.shakespeare`";
    TableId tableId = TableId.of(dataset, view);
    ViewDefinition viewDefinition = ViewDefinition.newBuilder(query).setUseLegacySql(false).build();
    runWithRetries(
        () -> bq.create(TableInfo.of(tableId, viewDefinition)),
        "createView(" + dataset + "." + view + ")");
  }

  static void createView(String dataset, String table, String view) {
    BigQuery bq = getBigquery();
    String query = String.format("SELECT * FROM `%s.%s`", dataset, table);
    TableId tableId = TableId.of(dataset, view);
    ViewDefinition viewDefinition = ViewDefinition.newBuilder(query).setUseLegacySql(false).build();
    runWithRetries(
        () -> bq.create(TableInfo.of(tableId, viewDefinition)),
        "createView(" + dataset + "." + view + ")");
  }

  public static void compareRows(Row row, Row expected) {
    for (int i = 0; i < expected.length(); i++) {
      // if (i == TestConstants.BIG_NUMERIC_COLUMN_POSITION) {
      // TODO: Restore this code after
      //  https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/446
      //  is fixed
      //
      // for (int j = 0; j < 2; j++) {
      //   String bigNumericString = getBigNumericString(row, i, j);
      //   String expectedBigNumericString = getBigNumericString(expected, i, j);
      //   assertThat(bigNumericString).isEqualTo(expectedBigNumericString);
      // }
      // } else {
      Object value = row.get(i);
      assertWithMessage("value of field " + expected.schema().fields()[i])
          .that(value)
          .isEqualTo(expected.get(i));
      //     }
    }
  }

  public static void compareBigNumericDataSetSchema(
      StructType actualSchema, StructType expectedSchema) {

    StructField[] actualFields = actualSchema.fields();
    StructField[] expectedFields = expectedSchema.fields();

    for (int i = 0; i < actualFields.length; i++) {
      StructField actualField = actualFields[i];
      StructField expectedField = expectedFields[i];
      assertThat(actualField).isEqualTo(expectedField);
      // TODO: Restore this code after
      //  https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/446
      //  is fixed
      //
      // if (i == TestConstants.BIG_NUMERIC_COLUMN_POSITION) {
      //   for (int j = 0; j < 2; j++) {
      //     DataType actualFieldDataType = ((StructType) actualField.dataType()).fields()[j]
      //         .dataType();
      //     DataType expectedFieldDataType = ((StructType) expectedField.dataType()).fields()[j]
      //         .dataType();
      //     assertThat(actualFieldDataType).isEqualTo(DataTypes.StringType);
      //     assertThat(expectedFieldDataType).isEqualTo(BigQueryDataTypes.BigNumericType);
      //   }
      //
      // } else {
      //   assertThat(actualField).isEqualTo(expectedField);
      // }
    }
  }

  public static void pollUntil(java.util.function.BooleanSupplier condition, int timeoutSeconds)
      throws Exception {
    long deadline =
        System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(timeoutSeconds);
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return; // Condition satisfied
      }
      Thread.sleep(100); // Check every 100ms
    }
    throw new java.util.concurrent.TimeoutException(
        "Condition not met within " + timeoutSeconds + " seconds.");
  }

  public static SparkSession.Builder createSparkSessionBuilder(String appName) {
    SparkSession.Builder builder = SparkSession.builder().appName(appName);
    if (System.getProperty("spark.master") == null && System.getenv("SPARK_MASTER") == null) {
      builder.master("local[*]");
    }
    return builder;
  }
}
