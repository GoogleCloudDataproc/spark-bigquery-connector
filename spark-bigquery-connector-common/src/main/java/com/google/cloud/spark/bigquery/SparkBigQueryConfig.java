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

import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.DEFAULT_FALLBACK;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.defaultBilledProject;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.empty;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.fromJavaUtil;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.getAnyBooleanOption;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.getAnyOption;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.getOption;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.getOptionFromMultipleParams;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.getRequiredOption;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.firstPresent;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.parseTableId;
import static com.google.cloud.spark.bigquery.SparkBigQueryUtil.scalaMapToJavaMap;
import static java.lang.String.format;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.ParquetOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConfig;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.connector.common.BigQueryProxyConfig;
import com.google.cloud.bigquery.connector.common.MaterializationConfiguration;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfigBuilder;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.threeten.bp.Duration;

public class SparkBigQueryConfig
    implements BigQueryConfig, BigQueryClient.LoadDataOptions, Serializable {

  public static final int MAX_TRACE_ID_LENGTH = 256;

  public enum WriteMethod {
    DIRECT,
    INDIRECT;

    public static WriteMethod from(@Nullable String writeMethod) {
      try {
        return WriteMethod.valueOf(writeMethod.toUpperCase(Locale.ENGLISH));
      } catch (RuntimeException e) {
        throw new IllegalArgumentException(
            "WriteMethod can be only " + Arrays.toString(WriteMethod.values()));
      }
    }
  }

  public static final String VIEWS_ENABLED_OPTION = "viewsEnabled";
  public static final String USE_AVRO_LOGICAL_TYPES_OPTION = "useAvroLogicalTypes";
  public static final String DATE_PARTITION_PARAM = "datePartition";
  public static final String VALIDATE_SPARK_AVRO_PARAM = "validateSparkAvroInternalParam";
  public static final String ENABLE_LIST_INFERENCE = "enableListInference";
  public static final String INTERMEDIATE_FORMAT_OPTION = "intermediateFormat";
  public static final String WRITE_METHOD_PARAM = "writeMethod";
  @VisibleForTesting static final DataFormat DEFAULT_READ_DATA_FORMAT = DataFormat.ARROW;

  @VisibleForTesting
  static final IntermediateFormat DEFAULT_INTERMEDIATE_FORMAT = IntermediateFormat.PARQUET;

  @VisibleForTesting
  static final CompressionCodec DEFAULT_ARROW_COMPRESSION_CODEC =
      CompressionCodec.COMPRESSION_UNSPECIFIED;

  static final String GCS_CONFIG_CREDENTIALS_FILE_PROPERTY =
      "google.cloud.auth.service.account.json.keyfile";
  static final String GCS_CONFIG_PROJECT_ID_PROPERTY = "fs.gs.project.id";
  private static final String READ_DATA_FORMAT_OPTION = "readDataFormat";
  private static final ImmutableList<String> PERMITTED_READ_DATA_FORMATS =
      ImmutableList.of(DataFormat.ARROW.toString(), DataFormat.AVRO.toString());
  private static final String CONF_PREFIX = "spark.datasource.bigquery.";
  private static final int DEFAULT_BIGQUERY_CLIENT_CONNECT_TIMEOUT = 60 * 1000;
  private static final int DEFAULT_BIGQUERY_CLIENT_READ_TIMEOUT = 60 * 1000;
  private static final Pattern LOWERCASE_QUERY_PATTERN = Pattern.compile("^(select|with)\\s+.*$");
  // Both MIN values correspond to the lower possible value that will actually make the code work.
  // 0 or less would make code hang or other bad side effects.
  public static final int MIN_BUFFERED_RESPONSES_PER_STREAM = 1;
  public static final int MIN_STREAMS_PER_PARTITION = 1;
  private static final int DEFAULT_BIGQUERY_CLIENT_RETRIES = 10;
  private static final String ARROW_COMPRESSION_CODEC_OPTION = "arrowCompressionCodec";
  private static final WriteMethod DEFAULT_WRITE_METHOD = WriteMethod.INDIRECT;
  public static final int DEFAULT_CACHE_EXPIRATION_IN_MINUTES = 15;
  static final String BIGQUERY_JOB_LABEL_PREFIX = "bigQueryJobLabel.";
  static final String BIGQUERY_TABLE_LABEL_PREFIX = "bigQueryTableLabel.";

  TableId tableId;
  // as the config needs to be Serializable, internally it uses
  // com.google.common.base.Optional<String> but externally it uses the regular java.util.Optional
  com.google.common.base.Optional<String> query = empty();
  String parentProjectId;
  boolean useParentProjectForMetadataOperations;
  com.google.common.base.Optional<String> accessTokenProviderFQCN;
  com.google.common.base.Optional<String> accessTokenProviderConfig;
  com.google.common.base.Optional<String> credentialsKey;
  com.google.common.base.Optional<String> credentialsFile;
  com.google.common.base.Optional<String> accessToken;
  com.google.common.base.Optional<String> filter = empty();
  com.google.common.base.Optional<StructType> schema = empty();
  Integer maxParallelism = null;
  Integer preferredMinParallelism = null;
  int defaultParallelism = 1;
  com.google.common.base.Optional<String> temporaryGcsBucket = empty();
  com.google.common.base.Optional<String> persistentGcsBucket = empty();
  com.google.common.base.Optional<String> persistentGcsPath = empty();

  IntermediateFormat intermediateFormat = DEFAULT_INTERMEDIATE_FORMAT;
  DataFormat readDataFormat = DEFAULT_READ_DATA_FORMAT;
  boolean combinePushedDownFilters = true;
  boolean viewsEnabled = false;
  com.google.common.base.Optional<String> materializationProject = empty();
  com.google.common.base.Optional<String> materializationDataset = empty();
  com.google.common.base.Optional<String> partitionField = empty();
  Long partitionExpirationMs = null;
  com.google.common.base.Optional<Boolean> partitionRequireFilter = empty();
  com.google.common.base.Optional<TimePartitioning.Type> partitionType = empty();
  com.google.common.base.Optional<String[]> clusteredFields = empty();
  com.google.common.base.Optional<JobInfo.CreateDisposition> createDisposition = empty();
  boolean optimizedEmptyProjection = true;
  boolean useAvroLogicalTypes = false;
  ImmutableList<String> decimalTargetTypes = ImmutableList.of();
  ImmutableList<JobInfo.SchemaUpdateOption> loadSchemaUpdateOptions = ImmutableList.of();
  int materializationExpirationTimeInMinutes;
  int maxReadRowsRetries = 3;
  boolean pushAllFilters = true;
  boolean enableModeCheckForSchemaFields = true;
  private com.google.common.base.Optional<String> encodedCreateReadSessionRequest = empty();
  private com.google.common.base.Optional<String> bigQueryStorageGrpcEndpoint = empty();
  private com.google.common.base.Optional<String> bigQueryHttpEndpoint = empty();
  private int numBackgroundThreadsPerStream = 0;
  private int numPrebufferReadRowsResponses = MIN_BUFFERED_RESPONSES_PER_STREAM;
  private int numStreamsPerPartition = MIN_STREAMS_PER_PARTITION;
  private SparkBigQueryProxyAndHttpConfig sparkBigQueryProxyAndHttpConfig;
  private CompressionCodec arrowCompressionCodec = DEFAULT_ARROW_COMPRESSION_CODEC;
  private WriteMethod writeMethod = DEFAULT_WRITE_METHOD;
  // for V2 write with BigQuery Storage Write API
  RetrySettings bigqueryDataWriteHelperRetrySettings =
      RetrySettings.newBuilder().setMaxAttempts(5).build();
  private int cacheExpirationTimeInMinutes = DEFAULT_CACHE_EXPIRATION_IN_MINUTES;
  // used to create BigQuery ReadSessions
  private com.google.common.base.Optional<String> traceId;
  private ImmutableMap<String, String> bigQueryJobLabels = ImmutableMap.of();
  private ImmutableMap<String, String> bigQueryTableLabels = ImmutableMap.of();
  private com.google.common.base.Optional<Long> createReadSessionTimeoutInSeconds;

  @VisibleForTesting
  SparkBigQueryConfig() {
    // empty
  }

  // new higher level method, as spark 3 need to parse the table specific options separately from
  // the catalog ones
  public static SparkBigQueryConfig from(
      Map<String, String> options,
      ImmutableMap<String, String> customDefaults,
      DataSourceVersion dataSourceVersion,
      SparkSession spark,
      Optional<StructType> schema,
      boolean tableIsMandatory) {
    Map<String, String> optionsMap = new HashMap<>(options);
    dataSourceVersion.updateOptionsMap(optionsMap);
    return SparkBigQueryConfig.from(
        ImmutableMap.copyOf(optionsMap),
        ImmutableMap.copyOf(scalaMapToJavaMap(spark.conf().getAll())),
        spark.sparkContext().hadoopConfiguration(),
        customDefaults,
        spark.sparkContext().defaultParallelism(),
        spark.sqlContext().conf(),
        spark.version(),
        schema,
        tableIsMandatory);
  }

  @VisibleForTesting
  public static SparkBigQueryConfig from(
      Map<String, String> optionsInput,
      ImmutableMap<String, String> originalGlobalOptions,
      Configuration hadoopConfiguration,
      ImmutableMap<String, String> customDefaults,
      int defaultParallelism,
      SQLConf sqlConf,
      String sparkVersion,
      Optional<StructType> schema,
      boolean tableIsMandatory) {
    SparkBigQueryConfig config = new SparkBigQueryConfig();

    ImmutableMap<String, String> options = toLowerCaseKeysMap(optionsInput);
    ImmutableMap<String, String> globalOptions = normalizeConf(originalGlobalOptions);
    config.sparkBigQueryProxyAndHttpConfig =
        SparkBigQueryProxyAndHttpConfig.from(options, globalOptions, hadoopConfiguration);
    // Issue #247
    // we need those parameters in case a read from query is issued
    config.viewsEnabled = getAnyBooleanOption(globalOptions, options, VIEWS_ENABLED_OPTION, false);
    MaterializationConfiguration materializationConfiguration =
        MaterializationConfiguration.from(globalOptions, options);
    config.materializationProject = materializationConfiguration.getMaterializationProject();
    config.materializationDataset = materializationConfiguration.getMaterializationDataset();
    config.materializationExpirationTimeInMinutes =
        materializationConfiguration.getMaterializationExpirationTimeInMinutes();
    // get the table details
    com.google.common.base.Optional<String> fallbackDataset = config.materializationDataset;
    Optional<String> fallbackProject =
        com.google.common.base.Optional.fromNullable(
                hadoopConfiguration.get(GCS_CONFIG_PROJECT_ID_PROPERTY))
            .toJavaUtil();
    Optional<String> tableParam =
        getOptionFromMultipleParams(options, ImmutableList.of("table", "path"), DEFAULT_FALLBACK)
            .toJavaUtil();
    Optional<String> datasetParam = getOption(options, "dataset").or(fallbackDataset).toJavaUtil();
    Optional<String> projectParam =
        firstPresent(getOption(options, "project").toJavaUtil(), fallbackProject);
    config.partitionType =
        getOption(options, "partitionType").transform(TimePartitioning.Type::valueOf);
    Optional<String> datePartitionParam = getOption(options, DATE_PARTITION_PARAM).toJavaUtil();
    datePartitionParam.ifPresent(
        date -> validateDateFormat(date, config.getPartitionTypeOrDefault(), DATE_PARTITION_PARAM));
    // checking for query
    if (tableParam.isPresent()) {
      String tableParamStr = tableParam.get().trim().replaceAll("\\s+", " ");
      if (isQuery(tableParamStr)) {
        // it is a query in practice
        config.query = com.google.common.base.Optional.of(tableParamStr);
        config.tableId = parseTableId("QUERY", datasetParam, projectParam, datePartitionParam);
      } else {
        config.tableId =
            parseTableId(tableParamStr, datasetParam, projectParam, datePartitionParam);
      }
    } else {
      // no table has been provided, it is either a query or an error
      config.query = getOption(options, "query").transform(String::trim);
      if (config.query.isPresent()) {
        config.tableId = parseTableId("QUERY", datasetParam, projectParam, datePartitionParam);
      } else if (tableIsMandatory) {
        // No table nor query were set. We cannot go further.
        throw new IllegalArgumentException("No table has been specified");
      }
    }

    config.parentProjectId =
        getAnyOption(globalOptions, options, "parentProject").or(defaultBilledProject());
    config.useParentProjectForMetadataOperations =
        getAnyBooleanOption(globalOptions, options, "useParentProjectForMetadataOperations", false);
    config.accessTokenProviderFQCN = getAnyOption(globalOptions, options, "gcpAccessTokenProvider");
    config.accessTokenProviderConfig =
        getAnyOption(globalOptions, options, "gcpAccessTokenProviderConfig");
    config.accessToken = getAnyOption(globalOptions, options, "gcpAccessToken");
    config.credentialsKey = getAnyOption(globalOptions, options, "credentials");
    config.credentialsFile =
        fromJavaUtil(
            firstPresent(
                getAnyOption(globalOptions, options, "credentialsFile").toJavaUtil(),
                com.google.common.base.Optional.fromNullable(
                        hadoopConfiguration.get(GCS_CONFIG_CREDENTIALS_FILE_PROPERTY))
                    .toJavaUtil()));
    config.accessToken = getAnyOption(globalOptions, options, "gcpAccessToken");
    config.filter = getOption(options, "filter");
    config.schema = fromJavaUtil(schema);
    config.maxParallelism =
        getOptionFromMultipleParams(
                options, ImmutableList.of("maxParallelism", "parallelism"), DEFAULT_FALLBACK)
            .transform(Integer::valueOf)
            .orNull();
    config.preferredMinParallelism =
        getAnyOption(globalOptions, options, "preferredMinParallelism")
            .transform(Integer::valueOf)
            .orNull();
    config.defaultParallelism = defaultParallelism;
    config.temporaryGcsBucket = getAnyOption(globalOptions, options, "temporaryGcsBucket");
    config.persistentGcsBucket = getAnyOption(globalOptions, options, "persistentGcsBucket");
    config.persistentGcsPath = getOption(options, "persistentGcsPath");
    WriteMethod writeMethodDefault =
        Optional.ofNullable(customDefaults.get(WRITE_METHOD_PARAM))
            .map(WriteMethod::from)
            .orElse(DEFAULT_WRITE_METHOD);
    config.writeMethod =
        getAnyOption(globalOptions, options, WRITE_METHOD_PARAM)
            .transform(WriteMethod::from)
            .or(writeMethodDefault);

    boolean validateSparkAvro =
        config.writeMethod == WriteMethod.INDIRECT
            && Boolean.valueOf(getRequiredOption(options, VALIDATE_SPARK_AVRO_PARAM, () -> "true"));
    boolean enableListInferenceForParquetMode =
        getAnyBooleanOption(globalOptions, options, ENABLE_LIST_INFERENCE, false);
    String intermediateFormatOption =
        getAnyOption(globalOptions, options, INTERMEDIATE_FORMAT_OPTION)
            .transform(String::toLowerCase)
            .or(DEFAULT_INTERMEDIATE_FORMAT.getDataSource());
    config.intermediateFormat =
        IntermediateFormat.from(
            intermediateFormatOption,
            sparkVersion,
            sqlConf,
            validateSparkAvro,
            enableListInferenceForParquetMode);
    String readDataFormatParam =
        getAnyOption(globalOptions, options, READ_DATA_FORMAT_OPTION)
            .transform(String::toUpperCase)
            .or(DEFAULT_READ_DATA_FORMAT.toString());
    if (!PERMITTED_READ_DATA_FORMATS.contains(readDataFormatParam)) {
      throw new IllegalArgumentException(
          format(
              "Data read format '%s' is not supported. Supported formats are '%s'",
              readDataFormatParam, String.join(",", PERMITTED_READ_DATA_FORMATS)));
    }
    config.useAvroLogicalTypes =
        getAnyBooleanOption(globalOptions, options, USE_AVRO_LOGICAL_TYPES_OPTION, false);
    config.readDataFormat = DataFormat.valueOf(readDataFormatParam);
    config.combinePushedDownFilters =
        getAnyBooleanOption(globalOptions, options, "combinePushedDownFilters", true);

    config.partitionField = getOption(options, "partitionField");
    config.partitionExpirationMs =
        getOption(options, "partitionExpirationMs").transform(Long::valueOf).orNull();
    config.partitionRequireFilter =
        getOption(options, "partitionRequireFilter").transform(Boolean::valueOf);
    config.clusteredFields = getOption(options, "clusteredFields").transform(s -> s.split(","));

    config.createDisposition =
        getOption(options, "createDisposition")
            .transform(String::toUpperCase)
            .transform(JobInfo.CreateDisposition::valueOf);

    config.optimizedEmptyProjection =
        getAnyBooleanOption(globalOptions, options, "optimizedEmptyProjection", true);

    boolean allowFieldAddition =
        getAnyBooleanOption(globalOptions, options, "allowFieldAddition", false);
    boolean allowFieldRelaxation =
        getAnyBooleanOption(globalOptions, options, "allowFieldRelaxation", false);
    ImmutableList.Builder<JobInfo.SchemaUpdateOption> loadSchemaUpdateOptions =
        ImmutableList.builder();
    if (allowFieldAddition) {
      loadSchemaUpdateOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
    }
    if (allowFieldRelaxation) {
      loadSchemaUpdateOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION);
    }
    config.loadSchemaUpdateOptions = loadSchemaUpdateOptions.build();
    com.google.common.base.Optional<String[]> decimalTargetTypes =
        getOption(options, "decimalTargetTypes").transform(s -> s.split(","));
    if (decimalTargetTypes.isPresent()) {
      config.decimalTargetTypes = ImmutableList.copyOf(decimalTargetTypes.get());
    }
    config.bigQueryStorageGrpcEndpoint =
        getAnyOption(globalOptions, options, "bigQueryStorageGrpcEndpoint");
    config.bigQueryHttpEndpoint = getAnyOption(globalOptions, options, "bigQueryHttpEndpoint");
    config.encodedCreateReadSessionRequest =
        getAnyOption(globalOptions, options, "bqEncodedCreateReadSessionRequest");
    config.numBackgroundThreadsPerStream =
        getAnyOption(globalOptions, options, "bqBackgroundThreadsPerStream")
            .transform(Integer::parseInt)
            .or(0);
    config.pushAllFilters = getAnyBooleanOption(globalOptions, options, "pushAllFilters", true);
    config.enableModeCheckForSchemaFields =
        getAnyBooleanOption(globalOptions, options, "enableModeCheckForSchemaFields", true);
    config.numPrebufferReadRowsResponses =
        getAnyOption(globalOptions, options, "bqPrebufferResponsesPerStream")
            .transform(Integer::parseInt)
            .or(MIN_BUFFERED_RESPONSES_PER_STREAM);
    config.numStreamsPerPartition =
        getAnyOption(globalOptions, options, "bqNumStreamsPerPartition")
            .transform(Integer::parseInt)
            .or(MIN_STREAMS_PER_PARTITION);

    String arrowCompressionCodecParam =
        getAnyOption(globalOptions, options, ARROW_COMPRESSION_CODEC_OPTION)
            .transform(String::toUpperCase)
            .or(DEFAULT_ARROW_COMPRESSION_CODEC.toString());

    try {
      config.arrowCompressionCodec = CompressionCodec.valueOf(arrowCompressionCodecParam);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          format(
              "Compression codec '%s' for Arrow is not supported. Supported formats are %s",
              arrowCompressionCodecParam, Arrays.toString(CompressionCodec.values())));
    }

    config.cacheExpirationTimeInMinutes =
        getAnyOption(globalOptions, options, "cacheExpirationTimeInMinutes")
            .transform(Integer::parseInt)
            .or(DEFAULT_CACHE_EXPIRATION_IN_MINUTES);
    if (config.cacheExpirationTimeInMinutes < 0) {
      throw new IllegalArgumentException(
          "cacheExpirationTimeInMinutes must have a positive value, the configured value is "
              + config.cacheExpirationTimeInMinutes);
    }

    com.google.common.base.Optional<String> traceApplicationNameParam =
        getAnyOption(globalOptions, options, "traceApplicationName");
    config.traceId =
        traceApplicationNameParam.transform(
            traceApplicationName -> {
              String traceJobIdParam =
                  getAnyOption(globalOptions, options, "traceJobId")
                      .or(SparkBigQueryUtil.getJobId(sqlConf));
              String traceIdParam = "Spark:" + traceApplicationName + ":" + traceJobIdParam;
              if (traceIdParam.length() > MAX_TRACE_ID_LENGTH) {
                throw new IllegalArgumentException(
                    String.format(
                        "trace ID cannot longer than %d. Provided value was [%s]",
                        MAX_TRACE_ID_LENGTH, traceIdParam));
              }
              return traceIdParam;
            });

    config.bigQueryJobLabels =
        parseBigQueryLabels(globalOptions, options, BIGQUERY_JOB_LABEL_PREFIX);
    config.bigQueryTableLabels =
        parseBigQueryLabels(globalOptions, options, BIGQUERY_TABLE_LABEL_PREFIX);

    config.createReadSessionTimeoutInSeconds =
        getAnyOption(globalOptions, options, "createReadSessionTimeoutInSeconds")
            .transform(Long::parseLong);
    return config;
  }

  @VisibleForTesting
  // takes only the options with the BIGQUERY_JOB_LABEL_PREFIX prefix, and strip them of this
  // prefix.
  // The `options` map overrides the `globalOptions` map.
  static ImmutableMap<String, String> parseBigQueryLabels(
      ImmutableMap<String, String> globalOptions,
      ImmutableMap<String, String> options,
      String labelPrefix) {

    String lowerCasePrefix = labelPrefix.toLowerCase(Locale.ROOT);

    ImmutableMap<String, String> allOptions =
        ImmutableMap.<String, String>builder() //
            .putAll(globalOptions) //
            .putAll(options) //
            .buildKeepingLast();

    ImmutableMap.Builder<String, String> result = ImmutableMap.<String, String>builder();
    for (Map.Entry<String, String> entry : allOptions.entrySet()) {
      if (entry.getKey().toLowerCase(Locale.ROOT).startsWith(lowerCasePrefix)) {
        result.put(entry.getKey().substring(labelPrefix.length()), entry.getValue());
      }
    }

    return result.build();
  }

  private static ImmutableMap<String, String> toLowerCaseKeysMap(Map<String, String> map) {
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      result.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
    }
    return ImmutableMap.copyOf(result);
  }

  @VisibleForTesting
  static boolean isQuery(String tableParamStr) {
    String potentialQuery = tableParamStr.toLowerCase().replace('\n', ' ');
    return LOWERCASE_QUERY_PATTERN.matcher(potentialQuery).matches();
  }

  private static void validateDateFormat(
      String date, TimePartitioning.Type partitionType, String optionName) {
    try {
      Map<TimePartitioning.Type, DateTimeFormatter> formatterMap =
          ImmutableMap.<TimePartitioning.Type, DateTimeFormatter>of(
              TimePartitioning.Type.HOUR, DateTimeFormatter.ofPattern("yyyyMMddHH"), //
              TimePartitioning.Type.DAY, DateTimeFormatter.BASIC_ISO_DATE, //
              TimePartitioning.Type.MONTH, DateTimeFormatter.ofPattern("yyyyMM"), //
              TimePartitioning.Type.YEAR, DateTimeFormatter.ofPattern("yyyy"));
      DateTimeFormatter dateTimeFormatter = formatterMap.get(partitionType);
      dateTimeFormatter.parse(date);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          String.format("Invalid argument for option %s, format is YYYYMMDD", optionName));
    }
  }

  static ImmutableMap<String, String> normalizeConf(Map<String, String> conf) {
    Map<String, String> normalizeConf =
        conf.entrySet().stream()
            .filter(e -> e.getKey().startsWith(CONF_PREFIX))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().substring(CONF_PREFIX.length()), e -> e.getValue()));
    Map<String, String> result = new HashMap<>(conf);
    result.putAll(normalizeConf);
    return ImmutableMap.copyOf(result);
  }

  public Credentials createCredentials() {

    return new BigQueryCredentialsSupplier(
            accessTokenProviderFQCN.toJavaUtil(),
            accessTokenProviderConfig.toJavaUtil(),
            accessToken.toJavaUtil(),
            credentialsKey.toJavaUtil(),
            credentialsFile.toJavaUtil(),
            sparkBigQueryProxyAndHttpConfig.getProxyUri(),
            sparkBigQueryProxyAndHttpConfig.getProxyUsername(),
            sparkBigQueryProxyAndHttpConfig.getProxyPassword())
        .getCredentials();
  }

  public TableId getTableId() {
    return tableId;
  }

  /** Returns the table id, without the added partition id if it exists. */
  public TableId getTableIdWithoutThePartition() {
    String tableAndPartition = tableId.getTable();
    if (!tableAndPartition.contains("$")) {
      // there is no partition id
      return tableId;
    }
    String table = tableAndPartition.substring(0, tableAndPartition.indexOf('$'));
    return tableId.getProject() != null
        ? TableId.of(tableId.getProject(), tableId.getDataset(), table)
        : TableId.of(tableId.getDataset(), table);
  }

  public Optional<String> getQuery() {
    return query.toJavaUtil();
  }

  @Override
  public String getParentProjectId() {
    return parentProjectId;
  }

  @Override
  public boolean useParentProjectForMetadataOperations() {
    return useParentProjectForMetadataOperations;
  }

  @Override
  public Optional<String> getAccessTokenProviderFQCN() {
    return accessTokenProviderFQCN.toJavaUtil();
  }

  @Override
  public Optional<String> getAccessTokenProviderConfig() {
    return accessTokenProviderConfig.toJavaUtil();
  }

  @Override
  public Optional<String> getCredentialsKey() {
    return credentialsKey.toJavaUtil();
  }

  @Override
  public Optional<String> getCredentialsFile() {
    return credentialsFile.toJavaUtil();
  }

  @Override
  public Optional<String> getAccessToken() {
    return accessToken.toJavaUtil();
  }

  public Optional<String> getFilter() {
    return filter.toJavaUtil();
  }

  public Optional<StructType> getSchema() {
    return schema.toJavaUtil();
  }

  public OptionalInt getMaxParallelism() {
    return maxParallelism == null ? OptionalInt.empty() : OptionalInt.of(maxParallelism);
  }

  public OptionalInt getPreferredMinParallelism() {
    return preferredMinParallelism == null
        ? OptionalInt.empty()
        : OptionalInt.of(preferredMinParallelism);
  }

  public int getDefaultParallelism() {
    return defaultParallelism;
  }

  public Optional<String> getTemporaryGcsBucket() {
    return temporaryGcsBucket.toJavaUtil();
  }

  public Optional<String> getPersistentGcsBucket() {
    return persistentGcsBucket.toJavaUtil();
  }

  public Optional<String> getPersistentGcsPath() {
    return persistentGcsPath.toJavaUtil();
  }

  public IntermediateFormat getIntermediateFormat() {
    return intermediateFormat;
  }

  public DataFormat getReadDataFormat() {
    return readDataFormat;
  }

  public CompressionCodec getArrowCompressionCodec() {
    return arrowCompressionCodec;
  }

  public boolean isCombinePushedDownFilters() {
    return combinePushedDownFilters;
  }

  @Override
  public boolean isUseAvroLogicalTypes() {
    return useAvroLogicalTypes;
  }

  public ImmutableList<String> getDecimalTargetTypes() {
    return decimalTargetTypes;
  }

  public boolean isViewsEnabled() {
    return viewsEnabled;
  }

  @Override
  public Optional<String> getMaterializationProject() {
    return materializationProject.toJavaUtil();
  }

  @Override
  public Optional<String> getMaterializationDataset() {
    return materializationDataset.toJavaUtil();
  }

  public Optional<String> getPartitionField() {
    return partitionField.toJavaUtil();
  }

  public OptionalLong getPartitionExpirationMs() {
    return partitionExpirationMs == null
        ? OptionalLong.empty()
        : OptionalLong.of(partitionExpirationMs);
  }

  public Optional<Boolean> getPartitionRequireFilter() {
    return partitionRequireFilter.toJavaUtil();
  }

  public Optional<TimePartitioning.Type> getPartitionType() {
    return partitionType.toJavaUtil();
  }

  public TimePartitioning.Type getPartitionTypeOrDefault() {
    return partitionType.or(TimePartitioning.Type.DAY);
  }

  public Optional<ImmutableList<String>> getClusteredFields() {
    return clusteredFields.transform(fields -> ImmutableList.copyOf(fields)).toJavaUtil();
  }

  public Optional<JobInfo.CreateDisposition> getCreateDisposition() {
    return createDisposition.toJavaUtil();
  }

  public boolean isOptimizedEmptyProjection() {
    return optimizedEmptyProjection;
  }

  public ImmutableList<JobInfo.SchemaUpdateOption> getLoadSchemaUpdateOptions() {
    return loadSchemaUpdateOptions;
  }

  public int getMaterializationExpirationTimeInMinutes() {
    return materializationExpirationTimeInMinutes;
  }

  public int getMaxReadRowsRetries() {
    return maxReadRowsRetries;
  }

  public boolean getPushAllFilters() {
    return pushAllFilters;
  }

  public boolean getEnableModeCheckForSchemaFields() {
    return enableModeCheckForSchemaFields;
  }

  // in order to simplify the configuration, the BigQuery client settings are fixed. If needed
  // we will add configuration properties for them.

  @Override
  public int getBigQueryClientConnectTimeout() {
    return sparkBigQueryProxyAndHttpConfig
        .getHttpConnectTimeout()
        .orElse(DEFAULT_BIGQUERY_CLIENT_CONNECT_TIMEOUT);
  }

  @Override
  public int getBigQueryClientReadTimeout() {
    return sparkBigQueryProxyAndHttpConfig
        .getHttpReadTimeout()
        .orElse(DEFAULT_BIGQUERY_CLIENT_READ_TIMEOUT);
  }

  @Override
  public BigQueryProxyConfig getBigQueryProxyConfig() {
    return sparkBigQueryProxyAndHttpConfig;
  }

  @Override
  public Optional<String> getBigQueryStorageGrpcEndpoint() {
    return bigQueryStorageGrpcEndpoint.toJavaUtil();
  }

  @Override
  public Optional<String> getBigQueryHttpEndpoint() {
    return bigQueryHttpEndpoint.toJavaUtil();
  }

  @Override
  public int getCacheExpirationTimeInMinutes() {
    return cacheExpirationTimeInMinutes;
  }

  @Override
  public Optional<Long> getCreateReadSessionTimeoutInSeconds() {
    return createReadSessionTimeoutInSeconds.toJavaUtil();
  }

  @Override
  public RetrySettings getBigQueryClientRetrySettings() {
    return RetrySettings.newBuilder()
        .setMaxAttempts(
            sparkBigQueryProxyAndHttpConfig
                .getHttpMaxRetry()
                .orElse(DEFAULT_BIGQUERY_CLIENT_RETRIES))
        .setTotalTimeout(Duration.ofMinutes(10))
        .setInitialRpcTimeout(Duration.ofSeconds(60))
        .setMaxRpcTimeout(Duration.ofMinutes(5))
        .setRpcTimeoutMultiplier(1.6)
        .setRetryDelayMultiplier(1.6)
        .setInitialRetryDelay(Duration.ofMillis(1250))
        .setMaxRetryDelay(Duration.ofSeconds(5))
        .build();
  }

  public RetrySettings getBigqueryDataWriteHelperRetrySettings() {
    return bigqueryDataWriteHelperRetrySettings;
  }

  public WriteMethod getWriteMethod() {
    return writeMethod;
  }

  public Optional<String> getTraceId() {
    return traceId.toJavaUtil();
  }

  @Override
  public ImmutableMap<String, String> getBigQueryJobLabels() {
    return bigQueryJobLabels;
  }

  public ImmutableMap<String, String> getBigQueryTableLabels() {
    return bigQueryTableLabels;
  }

  public ReadSessionCreatorConfig toReadSessionCreatorConfig() {
    return new ReadSessionCreatorConfigBuilder()
        .setViewsEnabled(viewsEnabled)
        .setMaterializationProject(materializationProject.toJavaUtil())
        .setMaterializationDataset(materializationDataset.toJavaUtil())
        .setMaterializationExpirationTimeInMinutes(materializationExpirationTimeInMinutes)
        .setReadDataFormat(readDataFormat)
        .setMaxReadRowsRetries(maxReadRowsRetries)
        .setViewEnabledParamName(VIEWS_ENABLED_OPTION)
        .setDefaultParallelism(defaultParallelism)
        .setMaxParallelism(getMaxParallelism())
        .setPreferredMinParallelism(getPreferredMinParallelism())
        .setRequestEncodedBase(encodedCreateReadSessionRequest.toJavaUtil())
        .setBigQueryStorageGrpcEndpoint(bigQueryStorageGrpcEndpoint.toJavaUtil())
        .setBigQueryHttpEndpoint(bigQueryHttpEndpoint.toJavaUtil())
        .setBackgroundParsingThreads(numBackgroundThreadsPerStream)
        .setPushAllFilters(pushAllFilters)
        .setPrebufferReadRowsResponses(numPrebufferReadRowsResponses)
        .setStreamsPerPartition(numStreamsPerPartition)
        .setArrowCompressionCodec(arrowCompressionCodec)
        .setTraceId(traceId.toJavaUtil())
        .build();
  }

  public BigQueryClient.ReadTableOptions toReadTableOptions() {
    return new BigQueryClient.ReadTableOptions() {
      @Override
      public TableId tableId() {
        return SparkBigQueryConfig.this.getTableId();
      }

      @Override
      public Optional<String> query() {
        return SparkBigQueryConfig.this.getQuery();
      }

      @Override
      public boolean viewsEnabled() {
        return SparkBigQueryConfig.this.isViewsEnabled();
      }

      @Override
      public String viewEnabledParamName() {
        return SparkBigQueryConfig.VIEWS_ENABLED_OPTION;
      }

      @Override
      public int expirationTimeInMinutes() {
        return SparkBigQueryConfig.this.getMaterializationExpirationTimeInMinutes();
      }
    };
  }

  public enum IntermediateFormat {
    AVRO("avro", FormatOptions.avro()),
    AVRO_2_3("com.databricks.spark.avro", FormatOptions.avro()),
    ORC("orc", FormatOptions.orc()),
    PARQUET("parquet", FormatOptions.parquet()),
    PARQUET_LIST_INFERENCE_ENABLED(
        "parquet", ParquetOptions.newBuilder().setEnableListInference(true).build());

    private static Set<String> PERMITTED_DATA_SOURCES =
        Stream.of(values())
            .map(IntermediateFormat::getDataSource)
            .filter(dataSource -> !dataSource.contains("."))
            .collect(Collectors.toSet());

    private final String dataSource;
    private final FormatOptions formatOptions;

    IntermediateFormat(String dataSource, FormatOptions formatOptions) {
      this.dataSource = dataSource;
      this.formatOptions = formatOptions;
    }

    public static IntermediateFormat from(
        String format,
        String sparkVersion,
        SQLConf sqlConf,
        boolean validateSparkAvro,
        boolean enableListInferenceForParquetMode) {
      Preconditions.checkArgument(
          PERMITTED_DATA_SOURCES.contains(format.toLowerCase()),
          "Data write format '%s' is not supported. Supported formats are %s",
          format,
          PERMITTED_DATA_SOURCES);

      if (validateSparkAvro && format.equalsIgnoreCase("avro")) {
        IntermediateFormat intermediateFormat = isSpark24OrAbove(sparkVersion) ? AVRO : AVRO_2_3;

        try {
          DataSource.lookupDataSource(intermediateFormat.getDataSource(), sqlConf);
        } catch (Exception ae) {
          throw missingAvroException(sparkVersion, ae);
        }

        return intermediateFormat;
      }

      if (enableListInferenceForParquetMode && format.equalsIgnoreCase("parquet")) {
        return PARQUET_LIST_INFERENCE_ENABLED;
      }

      // we have made sure that the format exist in the precondition, so findFirst() will
      // always find an instance
      return Stream.of(values())
          .filter(intermediateFormat -> intermediateFormat.getDataSource().equalsIgnoreCase(format))
          .findFirst()
          .get();
    }

    static boolean isSpark24OrAbove(String sparkVersion) {
      return sparkVersion.compareTo("2.4") > 0;
    }

    // could not load the spark-avro data source
    @VisibleForTesting
    static IllegalStateException missingAvroException(String sparkVersion, Exception cause) {
      String avroPackage;
      if (isSpark24OrAbove(sparkVersion)) {
        String scalaVersion = scala.util.Properties.versionNumberString();
        String scalaShortVersion = scalaVersion.substring(0, scalaVersion.lastIndexOf('.'));
        avroPackage =
            String.format("org.apache.spark:spark-avro_%s:%s", scalaShortVersion, sparkVersion);
      } else {
        avroPackage = "com.databricks:spark-avro_2.11:4.0.0";
      }
      String message =
          String.format(
              "Avro writing is not supported, as the spark-avro has not been found. "
                  + "Please re-run spark with the --packages %s parameter",
              avroPackage);

      return new IllegalStateException(message, cause);
    }

    public String getDataSource() {
      return dataSource;
    }

    public FormatOptions getFormatOptions() {
      return formatOptions;
    }

    public String getFileSuffix() {
      return getFormatOptions().getType().toLowerCase();
    }
  }
}
