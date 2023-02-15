/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery;

import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.DEFAULT_FALLBACK;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.getOptionFromMultipleParams;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.Metadata;
import scala.collection.Iterator;

/** Spark related utilities */
public class SparkBigQueryUtil {

  static final Properties BUILD_PROPERTIES = loadBuildProperties();

  static final String CONNECTOR_VERSION = BUILD_PROPERTIES.getProperty("connector.version");

  private static Properties loadBuildProperties() {
    try {
      Properties buildProperties = new Properties();
      buildProperties.load(
          SparkBigQueryUtil.class.getResourceAsStream("/spark-bigquery-connector.properties"));
      return buildProperties;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
  /**
   * Optimizing the URI list for BigQuery load, using the Spark specific file prefix and suffix
   * patterns, based on <code>BigQueryUtil.optimizeLoadUriList()</code>
   *
   * @param uris A list of URIs to be loaded by BigQuery load
   * @return an optimized list of URIs
   */
  public static List<String> optimizeLoadUriListForSpark(List<String> uris) {
    return BigQueryUtil.optimizeLoadUriList(uris, ".*/part-", "-[-\\w\\.]+");
  }

  /**
   * Checks whether temporaryGcsBucket or persistentGcsBucket parameters are present in the config
   * and creates a org.apache.hadoop.fs.Path object backed by GCS. When the indirect write method in
   * dsv1 is used, the data is written first to this GCS path and is then loaded into BigQuery
   *
   * @param config SparkBigQueryConfig
   * @param conf Hadoop configuration parameters
   * @param applicationId A unique identifier for the Spark application
   * @return org.apache.hadoop.fs.Path object backed by GCS
   */
  public static Path createGcsPath(
      SparkBigQueryConfig config, Configuration conf, String applicationId) {
    Path gcsPath;
    try {
      Preconditions.checkArgument(
          config.getTemporaryGcsBucket().isPresent() || config.getPersistentGcsBucket().isPresent(),
          "Either temporary or persistent GCS bucket must be set");

      // Throw exception if persistentGcsPath already exists in persistentGcsBucket
      if (config.getPersistentGcsBucket().isPresent()
          && config.getPersistentGcsPath().isPresent()) {
        gcsPath =
            new Path(
                String.format(
                    "gs://%s/%s",
                    config.getPersistentGcsBucket().get(), config.getPersistentGcsPath().get()));
        FileSystem fs = gcsPath.getFileSystem(conf);
        if (fs.exists(gcsPath)) {
          throw new IllegalArgumentException(
              String.format(
                  "Path %s already exists in %s bucket",
                  config.getPersistentGcsPath().get(), config.getPersistentGcsBucket().get()));
        }
      } else if (config.getTemporaryGcsBucket().isPresent()) {
        gcsPath = getUniqueGcsPath(config.getTemporaryGcsBucket().get(), applicationId, conf);
      } else {
        gcsPath = getUniqueGcsPath(config.getPersistentGcsBucket().get(), applicationId, conf);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return gcsPath;
  }

  private static Path getUniqueGcsPath(String gcsBucket, String applicationId, Configuration conf)
      throws IOException {
    boolean needNewPath = true;
    Path gcsPath = null;
    while (needNewPath) {
      gcsPath =
          new Path(
              String.format(
                  "gs://%s/.spark-bigquery-%s-%s", gcsBucket, applicationId, UUID.randomUUID()));
      FileSystem fs = gcsPath.getFileSystem(conf);
      needNewPath = fs.exists(gcsPath);
    }

    return gcsPath;
  }

  public static String getJobId(SQLConf sqlConf) {
    return getJobIdInternal(
        sqlConf.getConfString("spark.yarn.tags", "missing"),
        sqlConf.getConfString("spark.app.id", "generated-" + UUID.randomUUID()));
  }

  @VisibleForTesting
  // try to extract the dataproc job first, if not than use the applicationId
  static String getJobIdInternal(String yarnTags, String applicationId) {
    return Stream.of(yarnTags.split(","))
        .filter(tag -> tag.startsWith("dataproc_job_"))
        .findFirst()
        .orElseGet(() -> applicationId);
  }

  public static JobInfo.WriteDisposition saveModeToWriteDisposition(SaveMode saveMode) {
    if (saveMode == SaveMode.ErrorIfExists) {
      return JobInfo.WriteDisposition.WRITE_EMPTY;
    }
    // SaveMode.Ignore is handled in the data source level. If it has arrived here it means tha
    // table does not exist
    if (saveMode == SaveMode.Append || saveMode == SaveMode.Ignore) {
      return JobInfo.WriteDisposition.WRITE_APPEND;
    }
    if (saveMode == SaveMode.Overwrite) {
      return JobInfo.WriteDisposition.WRITE_TRUNCATE;
    }
    throw new IllegalArgumentException("SaveMode " + saveMode + " is currently not supported.");
  }

  public static TableId parseSimpleTableId(SparkSession spark, Map<String, String> options) {
    ImmutableMap<String, String> globalOptions =
        ImmutableMap.copyOf(scalaMapToJavaMap(spark.conf().getAll()));
    return BigQueryConfigurationUtil.parseSimpleTableId(globalOptions, options);
  }

  public static long sparkTimestampToBigQuery(Object sparkValue) {
    if (sparkValue instanceof Long) {
      return ((Number) sparkValue).longValue();
    }
    // need to return timestamp in epoch microseconds
    java.sql.Timestamp timestamp = (java.sql.Timestamp) sparkValue;
    long epochSecondsAsMicros = (timestamp.getTime() / 1000) * 1_000_000;
    int micros = timestamp.getNanos() / 1000;
    return epochSecondsAsMicros + micros;
  }

  public static int sparkDateToBigQuery(Object sparkValue) {
    if (sparkValue instanceof Number) {
      return ((Number) sparkValue).intValue();
    }
    java.sql.Date sparkDate = (java.sql.Date) sparkValue;
    return (int) sparkDate.toLocalDate().toEpochDay();
  }

  public static String getTableNameFromOptions(Map<String, String> options) {
    // options.get("table") when the "table" option is used, options.get("path") is set when
    // .load("table_name) is used
    Optional<String> tableParam =
        getOptionFromMultipleParams(options, ImmutableList.of("table", "path"), DEFAULT_FALLBACK)
            .toJavaUtil();
    String tableParamStr = tableParam.get().trim().replaceAll("\\s+", " ");
    TableId tableId = BigQueryUtil.parseTableId(tableParamStr);
    return BigQueryUtil.friendlyTableName(tableId);
  }

  // scala version agnostic conversion, that's why no JavaConverters are used
  public static <K, V> ImmutableMap<K, V> scalaMapToJavaMap(
      scala.collection.immutable.Map<K, V> map) {
    ImmutableMap.Builder<K, V> result = ImmutableMap.<K, V>builder();
    Iterator<K> keysIterator = map.keys().iterator();
    while (keysIterator.hasNext()) {
      K key = keysIterator.next();
      result.put(key, map.get(key).get());
    }
    return result.build();
  }

  public static boolean isDataFrameShowMethodInStackTrace() {
    for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
      if (stackTraceElement.getClassName().equals("org.apache.spark.sql.Dataset")
          && stackTraceElement.getMethodName().equals("showString")) {
        return true;
      }
    }

    return false;
  }

  public static boolean isJson(Metadata metadata) {
    return metadata.contains("sqlType") && "JSON".equals(metadata.getString("sqlType"));
  }

  public static ImmutableList<Filter> extractPartitionAndClusteringFilters(
      TableInfo table, ImmutableList<Filter> filters) {

    Optional<String> partitionField = BigQueryUtil.getPartitionField(table);
    ImmutableList<String> clusteringFields = BigQueryUtil.getClusteringFields(table);

    ImmutableList.Builder<String> filterFields = ImmutableList.builder();
    partitionField.ifPresent(filterFields::add);
    filterFields.addAll(clusteringFields);

    return filterFields.build().stream()
        .flatMap(field -> filtersOnField(filters, field))
        .collect(ImmutableList.toImmutableList());
  }

  @VisibleForTesting
  static Stream<Filter> filtersOnField(ImmutableList<Filter> filters, String field) {
    return filters.stream()
        .filter(
            filter ->
                Stream.of(filter.references()).anyMatch(reference -> reference.equals(field)));
  }
}
