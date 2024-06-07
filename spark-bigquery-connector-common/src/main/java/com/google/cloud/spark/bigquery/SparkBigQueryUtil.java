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
import static scala.collection.JavaConverters.mapAsJavaMap;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import scala.Option;

/** Spark related utilities */
public class SparkBigQueryUtil {

  private static final String SPARK_YARN_TAGS = "spark.yarn.tags";

  static final Properties BUILD_PROPERTIES = loadBuildProperties();

  static final String CONNECTOR_VERSION = BUILD_PROPERTIES.getProperty("connector.version");

  private static final ImmutableSet<TypeConverter> typeConverters;

  static {
    ServiceLoader<TypeConverter> serviceLoader = ServiceLoader.load(TypeConverter.class);
    typeConverters = ImmutableSet.copyOf(serviceLoader.iterator());
  }

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
                    "%s/%s",
                    getBucketAndScheme(config.getPersistentGcsBucket().get()),
                    config.getPersistentGcsPath().get()));
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
    String gcsBucketAndScheme = getBucketAndScheme(gcsBucket);
    while (needNewPath) {
      gcsPath =
          new Path(
              String.format(
                  "%s/.spark-bigquery-%s-%s",
                  gcsBucketAndScheme, applicationId, UUID.randomUUID()));
      FileSystem fs = gcsPath.getFileSystem(conf);
      needNewPath = fs.exists(gcsPath);
    }

    return gcsPath;
  }

  private static String getBucketAndScheme(String gcsBucket) {
    return gcsBucket.contains("://") ? gcsBucket : "gs://" + gcsBucket;
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
    long epochMillis = timestamp.getTime();
    int micros = (timestamp.getNanos() / 1000) % 1000;
    return epochMillis * 1000 + micros;
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
    map.foreach(entry -> result.put(entry._1(), entry._2()));
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

    ImmutableList<String> partitionFields = BigQueryUtil.getPartitionFields(table);
    ImmutableList<String> clusteringFields = BigQueryUtil.getClusteringFields(table);

    ImmutableList.Builder<String> filterFields = ImmutableList.builder();
    filterFields.addAll(partitionFields);
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

  public static Stream<TypeConverter> getTypeConverterStream() {
    return typeConverters.stream();
  }

  @NotNull
  public static ImmutableMap<String, String> extractJobLabels(SparkConf sparkConf) {
    Builder<String, String> labels = ImmutableMap.builder();
    ImmutableList<String> tags =
        Stream.of(Optional.ofNullable(sparkConf.get(SPARK_YARN_TAGS, null)))
            .filter(Optional::isPresent)
            .flatMap(value -> Stream.of(value.get().split(",")))
            .collect(ImmutableList.toImmutableList());
    tags.stream()
        .filter(tag -> tag.startsWith("dataproc_job_"))
        .findFirst()
        .ifPresent(
            tag ->
                labels.put(
                    "dataproc_job_id",
                    BigQueryUtil.sanitizeLabelValue(tag.substring(tag.lastIndexOf('_') + 1))));
    tags.stream()
        .filter(tag -> tag.startsWith("dataproc_uuid_"))
        .findFirst()
        .ifPresent(
            tag ->
                labels.put(
                    "dataproc_job_uuid",
                    BigQueryUtil.sanitizeLabelValue(tag.substring(tag.lastIndexOf('_') + 1))));
    return labels.build();
  }

  public static SparkBigQueryConfig createSparkBigQueryConfig(
      SQLContext sqlContext,
      scala.collection.immutable.Map<String, String> options,
      Option<StructType> schema,
      DataSourceVersion dataSourceVersion) {
    java.util.Map<String, String> optionsMap = new HashMap<>(mapAsJavaMap(options));
    dataSourceVersion.updateOptionsMap(optionsMap);
    SparkSession spark = sqlContext.sparkSession();
    ImmutableMap<String, String> globalOptions =
        ImmutableMap.copyOf(mapAsJavaMap(spark.conf().getAll()));
    int defaultParallelism =
        spark.sparkContext().isStopped() ? 1 : spark.sparkContext().defaultParallelism();

    return SparkBigQueryConfig.from(
        ImmutableMap.copyOf(optionsMap),
        globalOptions,
        spark.sparkContext().hadoopConfiguration(),
        ImmutableMap.of(),
        defaultParallelism,
        spark.sqlContext().conf(),
        spark.version(),
        Optional.ofNullable(schema.getOrElse(null)),
        true);
  }
}
