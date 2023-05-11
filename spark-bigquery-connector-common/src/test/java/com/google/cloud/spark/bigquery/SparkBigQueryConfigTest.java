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
package com.google.cloud.spark.bigquery;

import static com.google.cloud.spark.bigquery.SparkBigQueryConfig.BIGQUERY_JOB_LABEL_PREFIX;
import static com.google.cloud.spark.bigquery.SparkBigQueryConfig.BIGQUERY_TABLE_LABEL_PREFIX;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Assert;
import org.junit.Test;

public class SparkBigQueryConfigTest {

  public static final int DEFAULT_PARALLELISM = 10;
  public static final String SPARK_VERSION = "2.4.0";
  private static ImmutableMap<String, String> build;
  ImmutableMap<String, String> defaultOptions = ImmutableMap.of("table", "dataset.table");
  // "project", "test_project"); // to remove the need for default project

  @Test
  public void testSerializability() throws IOException {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options = new DataSourceOptions(defaultOptions);
    // test to make sure all members can be serialized.
    new ObjectOutputStream(new ByteArrayOutputStream())
        .writeObject(
            SparkBigQueryConfig.from(
                options.asMap(),
                ImmutableMap.of(),
                hadoopConfiguration,
                ImmutableMap.of(),
                DEFAULT_PARALLELISM,
                new SQLConf(),
                SPARK_VERSION,
                Optional.empty(), /* tableIsMandatory */
                true));
  }

  @Test
  public void testDefaults() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options = new DataSourceOptions(defaultOptions);
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            options.asMap(),
            ImmutableMap.of(),
            hadoopConfiguration,
            ImmutableMap.of(),
            DEFAULT_PARALLELISM,
            new SQLConf(),
            SPARK_VERSION,
            Optional.empty(), /* tableIsMandatory */
            true);
    assertThat(config.getTableId()).isEqualTo(TableId.of("dataset", "table"));
    assertThat(config.getFilter()).isEqualTo(Optional.empty());
    assertThat(config.getSchema()).isEqualTo(Optional.empty());
    assertThat(config.getMaxParallelism()).isEqualTo(OptionalInt.empty());
    assertThat(config.getPreferredMinParallelism()).isEqualTo(OptionalInt.empty());
    assertThat(config.getTemporaryGcsBucket()).isEqualTo(Optional.empty());
    assertThat(config.getIntermediateFormat())
        .isEqualTo(SparkBigQueryConfig.DEFAULT_INTERMEDIATE_FORMAT);
    assertThat(config.getReadDataFormat()).isEqualTo(SparkBigQueryConfig.DEFAULT_READ_DATA_FORMAT);
    assertThat(config.getMaterializationProject()).isEqualTo(Optional.empty());
    assertThat(config.getMaterializationDataset()).isEqualTo(Optional.empty());
    assertThat(config.getPartitionField()).isEqualTo(Optional.empty());
    assertThat(config.getPartitionExpirationMs()).isEqualTo(OptionalLong.empty());
    assertThat(config.getPartitionRequireFilter()).isEqualTo(Optional.empty());
    assertThat(config.getPartitionType()).isEqualTo(Optional.empty());
    assertThat(config.getClusteredFields()).isEqualTo(Optional.empty());
    assertThat(config.getCreateDisposition()).isEqualTo(Optional.empty());
    assertThat(config.getLoadSchemaUpdateOptions()).isEqualTo(ImmutableList.of());
    assertThat(config.getMaterializationExpirationTimeInMinutes()).isEqualTo(24 * 60);
    assertThat(config.getMaxReadRowsRetries()).isEqualTo(3);
    assertThat(config.isUseAvroLogicalTypes()).isFalse();
    assertThat(config.getDecimalTargetTypes()).isEmpty();
    assertThat(config.getBigQueryClientConnectTimeout()).isEqualTo(60 * 1000);
    assertThat(config.getBigQueryClientReadTimeout()).isEqualTo(60 * 1000);
    assertThat(config.getBigQueryClientRetrySettings().getMaxAttempts()).isEqualTo(10);
    assertThat(config.getArrowCompressionCodec())
        .isEqualTo(CompressionCodec.COMPRESSION_UNSPECIFIED);
    assertThat(config.getWriteMethod()).isEqualTo(SparkBigQueryConfig.WriteMethod.INDIRECT);
    assertThat(config.getCacheExpirationTimeInMinutes())
        .isEqualTo(SparkBigQueryConfig.DEFAULT_CACHE_EXPIRATION_IN_MINUTES);
    assertThat(config.getTraceId().isPresent()).isFalse();
    assertThat(config.getBigQueryJobLabels()).isEmpty();
    assertThat(config.getEnableModeCheckForSchemaFields()).isTrue();
    assertThat(config.getDatetimeZoneId()).isEqualTo(ZoneId.of("UTC"));
    assertThat(config.getQueryJobPriority()).isEqualTo(SparkBigQueryConfig.DEFAULT_JOB_PRIORITY);
    assertThat(config.getKmsKeyName()).isEqualTo(Optional.empty());
  }

  @Test
  public void testConfigFromOptions() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options =
        new DataSourceOptions(
            ImmutableMap.<String, String>builder()
                .put("table", "test_t")
                .put("dataset", "test_d")
                .put("project", "test_p")
                .put("filter", "test > 0")
                .put("parentProject", "test_pp")
                .put("maxParallelism", "99")
                .put("preferredMinParallelism", "10")
                .put("viewsEnabled", "true")
                .put("viewMaterializationProject", "vmp")
                .put("viewMaterializationDataset", "vmd")
                .put("materializationExpirationTimeInMinutes", "100")
                .put("readDataFormat", "ARROW")
                .put("optimizedEmptyProjection", "false")
                .put("createDisposition", "CREATE_NEVER")
                .put("temporaryGcsBucket", "some_bucket")
                .put("intermediateFormat", "ORC")
                .put("useAvroLogicalTypes", "true")
                .put("decimalTargetTypes", "NUMERIC,BIGNUMERIC")
                .put("partitionRequireFilter", "true")
                .put("partitionType", "HOUR")
                .put("partitionField", "some_field")
                .put("partitionExpirationMs", "999")
                .put("clusteredFields", "field1,field2")
                .put("allowFieldAddition", "true")
                .put("allowFieldRelaxation", "true")
                .put("httpConnectTimeout", "10000")
                .put("httpReadTimeout", "20000")
                .put("httpMaxRetry", "5")
                .put("arrowCompressionCodec", "ZSTD")
                .put("writeMethod", "direct")
                .put("cacheExpirationTimeInMinutes", "100")
                .put("traceJobId", "traceJobId")
                .put("traceApplicationName", "traceApplicationName")
                .put("bigQueryJobLabel.foo", "bar")
                .put("enableModeCheckForSchemaFields", "false")
                .put("datetimeZoneId", "Asia/Jerusalem")
                .put("queryJobPriority", "batch")
                .put("kmsKeyName", "some/key/name")
                .build());
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            options.asMap(),
            ImmutableMap.of(),
            hadoopConfiguration,
            ImmutableMap.of(),
            DEFAULT_PARALLELISM,
            new SQLConf(),
            SPARK_VERSION,
            Optional.empty(), /* tableIsMandatory */
            true);
    assertThat(config.getTableId()).isEqualTo(TableId.of("test_p", "test_d", "test_t"));
    assertThat(config.getFilter()).isEqualTo(Optional.of("test > 0"));
    assertThat(config.getSchema()).isEqualTo(Optional.empty());
    assertThat(config.getMaxParallelism()).isEqualTo(OptionalInt.of(99));
    assertThat(config.getPreferredMinParallelism()).isEqualTo(OptionalInt.of(10));
    assertThat(config.getTemporaryGcsBucket()).isEqualTo(Optional.of("some_bucket"));
    assertThat(config.getIntermediateFormat())
        .isEqualTo(SparkBigQueryConfig.IntermediateFormat.ORC);
    assertThat(config.getReadDataFormat()).isEqualTo(DataFormat.ARROW);
    assertThat(config.getMaterializationProject()).isEqualTo(Optional.of("vmp"));
    assertThat(config.getMaterializationDataset()).isEqualTo(Optional.of("vmd"));
    assertThat(config.getPartitionType()).isEqualTo(Optional.of(TimePartitioning.Type.HOUR));
    assertThat(config.getPartitionField()).isEqualTo(Optional.of("some_field"));
    assertThat(config.getPartitionExpirationMs()).isEqualTo(OptionalLong.of(999));
    assertThat(config.getPartitionRequireFilter()).isEqualTo(Optional.of(true));
    assertThat(config.getClusteredFields().get()).isEqualTo(ImmutableList.of("field1", "field2"));
    assertThat(config.getCreateDisposition())
        .isEqualTo(Optional.of(JobInfo.CreateDisposition.CREATE_NEVER));
    assertThat(config.getLoadSchemaUpdateOptions())
        .isEqualTo(
            ImmutableList.of(
                JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION));
    assertThat(config.getMaterializationExpirationTimeInMinutes()).isEqualTo(100);
    assertThat(config.getMaxReadRowsRetries()).isEqualTo(3);
    assertThat(config.isUseAvroLogicalTypes()).isTrue();
    assertThat(config.getDecimalTargetTypes()).isEqualTo(ImmutableList.of("NUMERIC", "BIGNUMERIC"));
    assertThat(config.getBigQueryClientConnectTimeout()).isEqualTo(10000);
    assertThat(config.getBigQueryClientReadTimeout()).isEqualTo(20000);
    assertThat(config.getBigQueryClientRetrySettings().getMaxAttempts()).isEqualTo(5);
    assertThat(config.getArrowCompressionCodec()).isEqualTo(CompressionCodec.ZSTD);
    assertThat(config.getWriteMethod()).isEqualTo(SparkBigQueryConfig.WriteMethod.DIRECT);
    assertThat(config.getCacheExpirationTimeInMinutes()).isEqualTo(100);
    assertThat(config.getTraceId()).isEqualTo(Optional.of("Spark:traceApplicationName:traceJobId"));
    assertThat(config.getBigQueryJobLabels()).hasSize(1);
    assertThat(config.getBigQueryJobLabels()).containsEntry("foo", "bar");
    assertThat(config.getEnableModeCheckForSchemaFields()).isFalse();
    assertThat(config.getDatetimeZoneId()).isEqualTo(ZoneId.of("Asia/Jerusalem"));
    assertThat(config.getQueryJobPriority()).isEqualTo(Priority.valueOf("BATCH"));
    assertThat(config.getKmsKeyName()).isEqualTo(Optional.of("some/key/name"));
  }

  @Test
  public void testCacheExpirationSetToZero() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options =
        new DataSourceOptions(
            ImmutableMap.<String, String>builder()
                .put("table", "test_t")
                .put("dataset", "test_d")
                .put("project", "test_p")
                .put("cacheExpirationTimeInMinutes", "0")
                .build());
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            options.asMap(),
            ImmutableMap.of(),
            hadoopConfiguration,
            ImmutableMap.of(),
            DEFAULT_PARALLELISM,
            new SQLConf(),
            SPARK_VERSION,
            Optional.empty(), /* tableIsMandatory */
            true);
    assertThat(config.getCacheExpirationTimeInMinutes()).isEqualTo(0);
  }

  @Test
  public void testCacheExpirationSetToNegative() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options =
        new DataSourceOptions(
            ImmutableMap.<String, String>builder()
                .put("table", "test_t")
                .put("dataset", "test_d")
                .put("project", "test_p")
                .put("cacheExpirationTimeInMinutes", "-1")
                .build());

    IllegalArgumentException exception =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
                SparkBigQueryConfig.from(
                    options.asMap(),
                    ImmutableMap.of(),
                    hadoopConfiguration,
                    ImmutableMap.of(),
                    DEFAULT_PARALLELISM,
                    new SQLConf(),
                    SPARK_VERSION,
                    Optional.empty(), /* tableIsMandatory */
                    true));

    assertThat(exception)
        .hasMessageThat()
        .contains(
            "cacheExpirationTimeInMinutes must have a positive value, the configured value is -1");
  }

  @Test
  public void testInvalidCompressionCodec() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options =
        new DataSourceOptions(
            ImmutableMap.<String, String>builder()
                .put("table", "test_t")
                .put("dataset", "test_d")
                .put("project", "test_p")
                .put("arrowCompressionCodec", "randomCompression")
                .build());

    IllegalArgumentException exception =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
                SparkBigQueryConfig.from(
                    options.asMap(),
                    ImmutableMap.of(),
                    hadoopConfiguration,
                    ImmutableMap.of(),
                    DEFAULT_PARALLELISM,
                    new SQLConf(),
                    SPARK_VERSION,
                    Optional.empty(), /* tableIsMandatory */
                    true));

    assertThat(exception)
        .hasMessageThat()
        .contains(
            "Compression codec 'RANDOMCOMPRESSION' for Arrow is not supported."
                + " Supported formats are "
                + Arrays.toString(CompressionCodec.values()));
  }

  @Test
  public void testConfigFromGlobalOptions() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options =
        new DataSourceOptions(
            ImmutableMap.<String, String>builder().put("table", "dataset.table").build());
    ImmutableMap<String, String> globalOptions =
        ImmutableMap.<String, String>builder()
            .put("viewsEnabled", "true")
            .put("spark.datasource.bigquery.temporaryGcsBucket", "bucket")
            .put("bigQueryStorageGrpcEndpoint", "bqsge")
            .put("bigQueryHttpEndpoint", "bqhe")
            .put("bqEncodedCreateReadSessionRequest", "ec")
            .put("writeMethod", "direct")
            .build();
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            options.asMap(),
            globalOptions,
            hadoopConfiguration,
            ImmutableMap.of(),
            DEFAULT_PARALLELISM,
            new SQLConf(),
            SPARK_VERSION,
            Optional.empty(), /* tableIsMandatory */
            true);

    assertThat(config.isViewsEnabled()).isTrue();
    assertThat(config.getTemporaryGcsBucket()).isEqualTo(Optional.of("bucket"));
    assertThat(config.toReadSessionCreatorConfig().getBigQueryStorageGrpcEndpoint().get())
        .isEqualTo("bqsge");
    assertThat(config.toReadSessionCreatorConfig().getBigQueryHttpEndpoint().get())
        .isEqualTo("bqhe");
    assertThat(config.toReadSessionCreatorConfig().getRequestEncodedBase().get()).isEqualTo("ec");
    assertThat(config.getWriteMethod()).isEqualTo(SparkBigQueryConfig.WriteMethod.DIRECT);
  }

  @Test
  public void testGetTableIdWithoutThePartition_PartitionExists() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options =
        new DataSourceOptions(
            ImmutableMap.of("table", "dataset.table", "datePartition", "20201010"));
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            options.asMap(),
            ImmutableMap.of(),
            hadoopConfiguration,
            ImmutableMap.of(),
            DEFAULT_PARALLELISM,
            new SQLConf(),
            SPARK_VERSION,
            Optional.empty(), /* tableIsMandatory */
            true);

    assertThat(config.getTableId().getTable()).isEqualTo("table$20201010");
    assertThat(config.getTableIdWithoutThePartition().getTable()).isEqualTo("table");
    assertThat(config.getTableIdWithoutThePartition().getDataset())
        .isEqualTo(config.getTableId().getDataset());
    assertThat(config.getTableIdWithoutThePartition().getProject())
        .isEqualTo(config.getTableId().getProject());
  }

  @Test
  public void testGetTableIdWithoutThePartition_PartitionMissing() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options = new DataSourceOptions(defaultOptions);
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            options.asMap(),
            ImmutableMap.of(),
            hadoopConfiguration,
            ImmutableMap.of(),
            DEFAULT_PARALLELISM,
            new SQLConf(),
            SPARK_VERSION,
            Optional.empty(), /* tableIsMandatory */
            true);

    assertThat(config.getTableIdWithoutThePartition().getTable())
        .isEqualTo(config.getTableId().getTable());
    assertThat(config.getTableIdWithoutThePartition().getDataset())
        .isEqualTo(config.getTableId().getDataset());
    assertThat(config.getTableIdWithoutThePartition().getProject())
        .isEqualTo(config.getTableId().getProject());
  }

  @Test
  public void testQueryMatching() {
    assertThat(SparkBigQueryConfig.isQuery("table")).isFalse();
    assertThat(SparkBigQueryConfig.isQuery("dataset.table")).isFalse();
    assertThat(SparkBigQueryConfig.isQuery("project.dataset.table")).isFalse();

    assertThat(SparkBigQueryConfig.isQuery("select a,b from table")).isTrue();
    assertThat(SparkBigQueryConfig.isQuery("SELECT\n a,b\nfrom table")).isTrue();
    assertThat(SparkBigQueryConfig.isQuery("SELECT\ta,b from table")).isTrue();
    assertThat(SparkBigQueryConfig.isQuery("WITH bar AS (SELECT * FROM foo)\nSELECT * FROM bar"))
        .isTrue();
  }

  @Test
  public void testJobLabelOverride() {
    ImmutableMap<String, String> globalOptions =
        ImmutableMap.<String, String>builder()
            .put("bigQueryJobLabel.foo", "1")
            .put("bigQueryJobLabel.bar", "1")
            .build();
    ImmutableMap<String, String> options =
        ImmutableMap.<String, String>builder()
            .put("bigQueryJobLabel.foo", "2")
            .put("bigQueryJobLabel.baz", "2")
            .build();
    ImmutableMap<String, String> labels =
        SparkBigQueryConfig.parseBigQueryLabels(globalOptions, options, BIGQUERY_JOB_LABEL_PREFIX);
    assertThat(labels).hasSize(3);
    assertThat(labels).containsEntry("foo", "2");
    assertThat(labels).containsEntry("bar", "1");
    assertThat(labels).containsEntry("baz", "2");
  }

  @Test
  public void testTableLabelOverride() {
    ImmutableMap<String, String> globalOptions =
        ImmutableMap.<String, String>builder()
            .put("bigQueryTableLabel.foo", "1")
            .put("bigQueryTableLabel.bar", "1")
            .build();
    ImmutableMap<String, String> options =
        ImmutableMap.<String, String>builder()
            .put("bigQueryTableLabel.foo", "2")
            .put("bigQueryTableLabel.baz", "2")
            .build();
    ImmutableMap<String, String> labels =
        SparkBigQueryConfig.parseBigQueryLabels(
            globalOptions, options, BIGQUERY_TABLE_LABEL_PREFIX);
    assertThat(labels).hasSize(3);
    assertThat(labels).containsEntry("foo", "2");
    assertThat(labels).containsEntry("bar", "1");
    assertThat(labels).containsEntry("baz", "2");
  }

  @Test
  public void testCustomDefaults() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options = new DataSourceOptions(defaultOptions);
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            options.asMap(),
            ImmutableMap.of(),
            hadoopConfiguration,
            ImmutableMap.of("writeMethod", "INDIRECT"),
            DEFAULT_PARALLELISM,
            new SQLConf(),
            SPARK_VERSION,
            Optional.empty(), /* tableIsMandatory */
            true);

    assertThat(config.getWriteMethod()).isEqualTo(SparkBigQueryConfig.WriteMethod.INDIRECT);
  }

  // ported from SparkBigQueryConfigSuite.scala

  static Configuration hadoopConfiguration = new Configuration();

  static {
    hadoopConfiguration.set(
        SparkBigQueryConfig.GCS_CONFIG_CREDENTIALS_FILE_PROPERTY, "hadoop_cfile");
    hadoopConfiguration.set(SparkBigQueryConfig.GCS_CONFIG_PROJECT_ID_PROPERTY, "hadoop_project");
  }

  static ImmutableMap<String, String> parameters = ImmutableMap.of("table", "dataset.table");
  static ImmutableMap<String, String> emptyMap = ImmutableMap.of();
  static String sparkVersion = "2.4.0";

  private static Map<String, String> asDataSourceOptionsMap(Map<String, String> map) {
    Map<String, String> result = new HashMap<>();
    result.putAll(map);
    for (Map.Entry<String, String> entry : map.entrySet()) {
      result.put(entry.getKey().toLowerCase(Locale.US), entry.getValue());
    }
    return ImmutableMap.copyOf(result);
  }

  private Map<String, String> withParameter(String key, String value) {
    return ImmutableMap.<String, String>builder().putAll(parameters).put(key, value).build();
  }

  private Map<String, String> withParameters(
      String key1, String value1, String key2, String value2) {
    return ImmutableMap.<String, String>builder()
        .putAll(parameters)
        .put(key1, value1)
        .put(key2, value2)
        .build();
  }

  @Test
  public void testTakingCredentialsFileFromGcsHadoopConfig() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(parameters),
            emptyMap, // allConf
            hadoopConfiguration,
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getCredentialsFile()).isEqualTo(Optional.of("hadoop_cfile"));
  }

  @Test
  public void testTakingCredentialsFilefromTheProperties() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("credentialsFile", "cfile")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getCredentialsFile()).isEqualTo(Optional.of("cfile"));
  }

  @Test
  public void testNoCredentialsFileIsProvided() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(parameters),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getCredentialsFile().isPresent()).isFalse();
  }

  @Test
  public void testTakingProjectIdFromGcsHadoopConfig() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(parameters),
            emptyMap, // allConf
            hadoopConfiguration,
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getTableId().getProject()).isEqualTo("hadoop_project");
  }

  @Test
  public void testTakingProjectIdFromTheProperties() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("project", "pid")),
            emptyMap, // allConf
            hadoopConfiguration,
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getTableId().getProject()).isEqualTo("pid");
  }

  @Test
  public void testNoProjectIdIsProvided() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(parameters),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getTableId().getProject()).isNull();
  }

  @Test
  public void testInvalidDataFormat() {
    try {
      SparkBigQueryConfig.from(
          asDataSourceOptionsMap(withParameter("readDataFormat", "abc")),
          emptyMap, // allConf
          new Configuration(),
          emptyMap, // customDefaults
          1,
          new SQLConf(),
          sparkVersion,
          /* schema */ Optional.empty(),
          /* tableIsMandatory */ true);
      fail("Should throw Exception");
    } catch (Exception e) {
      assertThat(e.getMessage())
          .isEqualTo("Data read format 'ABC' is not supported. Supported formats are 'ARROW,AVRO'");
    }
  }

  @Test
  public void testDataFormatNoValueIsSet() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(parameters),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getReadDataFormat()).isEqualTo(DataFormat.ARROW);
  }

  @Test
  public void testSetReadDataFormatAsAvro() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("readDataFormat", "Avro")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getReadDataFormat()).isEqualTo(DataFormat.AVRO);
  }

  @Test
  public void testGetAnyOptionWithFallbackOnlyNewConfigExist() {

    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("materializationProject", "foo")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getMaterializationProject()).isEqualTo(Optional.of("foo"));
  }

  @Test
  public void testGetAnyOptionWithFallbackBothConfigsExist() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(
                withParameters(
                    "materializationProject", "foo", "viewMaterializationProject", "bar")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getMaterializationProject()).isEqualTo(Optional.of("foo"));
  }

  @Test
  public void testGetAnyOptionWithFallbackOnlyOldConfigExist() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("viewMaterializationProject", "bar")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getMaterializationProject()).isEqualTo(Optional.of("bar"));
  }

  @Test
  public void testGetAnyOptionWithFallbackNoConfigExists() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(parameters),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getMaterializationProject()).isEqualTo(Optional.empty());
  }

  @Test
  public void testMaxParallelismOnlyNewConfigExist() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("maxParallelism", "3")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getMaxParallelism()).isEqualTo(OptionalInt.of(3));
  }

  @Test
  public void testMaxParallelismBothConfigsExist() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameters("maxParallelism", "3", "parallelism", "10")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getMaxParallelism()).isEqualTo(OptionalInt.of(3));
  }

  @Test
  public void testMaxParallelismOnlyOldConfigExists() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("parallelism", "10")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getMaxParallelism()).isEqualTo(OptionalInt.of(10));
  }

  @Test
  public void testMaxParallelismNoConfigExists() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(parameters),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getMaxParallelism()).isEqualTo(OptionalInt.empty());
  }

  @Test
  public void testLoadSchemaUpdateOptionAllowFieldAddition() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("allowFieldAddition", "true")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getLoadSchemaUpdateOptions())
        .contains(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
  }

  @Test
  public void testLoadSchemaUpdateOptionAllowFieldRelaxation() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("allowFieldRelaxation", "true")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getLoadSchemaUpdateOptions())
        .contains(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION);
  }

  @Test
  public void testLoadSchemaUpdateOptionBoth() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(
                withParameters("allowFieldAddition", "true", "allowFieldRelaxation", "true")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getLoadSchemaUpdateOptions())
        .containsAtLeast(
            JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION);
  }

  @Test
  public void testLoadSchemaUpdateOptionNone() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(parameters),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getLoadSchemaUpdateOptions()).isEmpty();
  }

  @Test
  public void testNormalizeAllConf() {
    Map<String, String> originalConf =
        ImmutableMap.of(
            "key1", "val1",
            "spark.datasource.bigquery.key2", "val2",
            "key3", "val3",
            "spark.datasource.bigquery.key3", "external val3");
    Map<String, String> normalizedConf = SparkBigQueryConfig.normalizeConf(originalConf);

    assertThat(normalizedConf.get("key1")).isEqualTo("val1");
    assertThat(normalizedConf.get("key2")).isEqualTo("val2");
    assertThat(normalizedConf.get("key3")).isEqualTo("external val3");
  }

  @Test
  public void testSetPersistentGcsPath() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("persistentGcsPath", "/persistent/path")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getPersistentGcsPath()).isEqualTo(Optional.of("/persistent/path"));
  }

  @Test
  public void testSetPersistentGcsBucket() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(
                withParameter("persistentGcsBucket", "gs://persistentGcsBucket")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getPersistentGcsBucket()).isEqualTo(Optional.of("gs://persistentGcsBucket"));
  }

  @Test
  public void testBqFlowControWindow() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("bqFlowControlWindowBytes", "12345")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getFlowControlWindowBytes()).isEqualTo(Optional.of(12345));
  }

  @Test
  public void testBadCredentials() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(
                withParameter(
                    "credentials",
                    Base64.getEncoder().encodeToString("{}".getBytes(StandardCharsets.UTF_8)))),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);

    Exception e = assertThrows(Exception.class, () -> config.createCredentials());
    assertThat(e.getMessage()).contains("Failed to create Credentials from key");
  }

  @Test
  public void testImpersonationGlobal() {
    String sa = "abc@example.iam.gserviceaccount.com";
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("gcpImpersonationServiceAccount", sa)),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);

    ImpersonatedCredentials credentials = (ImpersonatedCredentials) config.createCredentials();
    assertThat(credentials.getAccount()).isEqualTo(sa);
  }

  @Test
  public void testImpersonationGlobalForUser() {
    String user = "bob";
    String sa = "bob@example.iam.gserviceaccount.com";
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    ugi.doAs(
        (java.security.PrivilegedAction<Void>)
            () -> {
              SparkBigQueryConfig config =
                  SparkBigQueryConfig.from(
                      asDataSourceOptionsMap(
                          withParameter("gcpImpersonationServiceAccountForUser." + user, sa)),
                      emptyMap, // allConf
                      new Configuration(),
                      emptyMap, // customDefaults
                      1,
                      new SQLConf(),
                      sparkVersion,
                      /* schema */ Optional.empty(),
                      /* tableIsMandatory */ true);

              ImpersonatedCredentials credentials =
                  (ImpersonatedCredentials) config.createCredentials();
              assertThat(credentials.getAccount()).isEqualTo(sa);
              return null;
            });
  }

  @Test
  public void testImpersonationGlobalForGroup() {
    String user = "bob";
    String[] groups = new String[] {"datascience"};
    String sa = "datascience-team@example.iam.gserviceaccount.com";
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(user, groups);
    ugi.doAs(
        (java.security.PrivilegedAction<Void>)
            () -> {
              SparkBigQueryConfig config =
                  SparkBigQueryConfig.from(
                      asDataSourceOptionsMap(
                          withParameter("gcpImpersonationServiceAccountForGroup." + groups[0], sa)),
                      emptyMap, // allConf
                      new Configuration(),
                      emptyMap, // customDefaults
                      1,
                      new SQLConf(),
                      sparkVersion,
                      /* schema */ Optional.empty(),
                      /* tableIsMandatory */ true);

              ImpersonatedCredentials credentials =
                  (ImpersonatedCredentials) config.createCredentials();
              assertThat(credentials.getAccount()).isEqualTo(sa);
              return null;
            });
  }

  @Test
  public void testMissingAvroMessage() {
    Exception cause = new Exception("test");
    IllegalStateException before24 =
        SparkBigQueryConfig.IntermediateFormat.missingAvroException("2.3.5", cause);
    assertThat(before24.getMessage()).contains("com.databricks:spark-avro_2.11:4.0.0");
    IllegalStateException after24 =
        SparkBigQueryConfig.IntermediateFormat.missingAvroException("2.4.8", cause);
    assertThat(after24.getMessage()).contains("org.apache.spark:spark-avro_2.13:2.4.8");
  }

  @Test
  public void testEnableListInferenceWithDefaultIntermediateFormat() {
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            asDataSourceOptionsMap(withParameter("enableListInference", "true")),
            emptyMap, // allConf
            new Configuration(),
            emptyMap, // customDefaults
            1,
            new SQLConf(),
            sparkVersion,
            /* schema */ Optional.empty(),
            /* tableIsMandatory */ true);
    assertThat(config.getIntermediateFormat())
        .isEqualTo(SparkBigQueryConfig.IntermediateFormat.PARQUET_LIST_INFERENCE_ENABLED);
  }
}
