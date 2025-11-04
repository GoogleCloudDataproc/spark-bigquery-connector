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
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.getAnyOptionsWithPrefix;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.getOption;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.getOptionFromMultipleParams;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.getRequiredOption;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.removePrefixFromMapKeys;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.firstPresent;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.parseTableId;
import static com.google.cloud.spark.bigquery.SparkBigQueryUtil.scalaMapToJavaMap;
import static java.lang.String.format;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.ParquetOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConfig;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.connector.common.BigQueryProxyConfig;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.bigquery.connector.common.MaterializationConfiguration;
import com.google.cloud.bigquery.connector.common.QueryParameterHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfigBuilder;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.threeten.bp.Duration;

public class SparkBigQueryConfig
    implements BigQueryConfig,
        BigQueryClient.CreateTableOptions,
        BigQueryClient.LoadDataOptions,
        Serializable {

  private static final long serialVersionUID = 728392817473829L;

  public static final int MAX_TRACE_ID_LENGTH = 256;
  public static final TableId QUERY_DUMMY_TABLE_ID = TableId.of("QUERY", "QUERY");

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

  public static final String IMPERSONATION_GLOBAL = "gcpImpersonationServiceAccount";
  public static final String IMPERSONATION_FOR_USER_PREFIX =
      "gcpImpersonationServiceAccountForUser.";
  public static final String IMPERSONATION_FOR_GROUP_PREFIX =
      "gcpImpersonationServiceAccountForGroup.";

  public static final String VIEWS_ENABLED_OPTION = "viewsEnabled";
  public static final String USE_AVRO_LOGICAL_TYPES_OPTION = "useAvroLogicalTypes";
  public static final String DATE_PARTITION_PARAM = "datePartition";
  public static final String VALIDATE_SPARK_AVRO_PARAM = "validateSparkAvroInternalParam";
  public static final String ENABLE_LIST_INFERENCE = "enableListInference";
  public static final String INTERMEDIATE_FORMAT_OPTION = "intermediateFormat";
  public static final String WRITE_METHOD_PARAM = "writeMethod";
  public static final String WRITE_AT_LEAST_ONCE_OPTION = "writeAtLeastOnce";
  @VisibleForTesting static final DataFormat DEFAULT_READ_DATA_FORMAT = DataFormat.ARROW;

  @VisibleForTesting
  static final IntermediateFormat DEFAULT_INTERMEDIATE_FORMAT = IntermediateFormat.PARQUET;

  @VisibleForTesting
  static final CompressionCodec DEFAULT_ARROW_COMPRESSION_CODEC =
      CompressionCodec.COMPRESSION_UNSPECIFIED;

  @VisibleForTesting
  static final ResponseCompressionCodec DEFAULT_RESPONSE_COMPRESSION_CODEC =
      ResponseCompressionCodec.RESPONSE_COMPRESSION_CODEC_UNSPECIFIED;

  static final String GCS_CONFIG_CREDENTIALS_FILE_PROPERTY =
      "google.cloud.auth.service.account.json.keyfile";
  static final String GCS_CONFIG_PROJECT_ID_PROPERTY = "fs.gs.project.id";
  private static final String READ_DATA_FORMAT_OPTION = "readDataFormat";
  private static final ImmutableList<String> PERMITTED_READ_DATA_FORMATS =
      ImmutableList.of(DataFormat.ARROW.toString(), DataFormat.AVRO.toString());
  private static final String CONF_PREFIX = "spark.datasource.bigquery.";
  private static final int DEFAULT_BIGQUERY_CLIENT_CONNECT_TIMEOUT = 60 * 1000;
  private static final int DEFAULT_BIGQUERY_CLIENT_READ_TIMEOUT = 60 * 1000;
  private static final Pattern QUICK_LOWERCASE_QUERY_PATTERN =
      Pattern.compile("(?i)^\\s*(select|with|\\()\\b[\\s\\S]*");
  private static final Pattern HAS_WHITESPACE_PATTERN = Pattern.compile("\\s");
  private static final Pattern SQL_KEYWORD_PATTERN =
      Pattern.compile("(?i)\\b(select|from|where|join|group by|order by|union all)\\b");
  // Both MIN values correspond to the lower possible value that will actually make the code work.
  // 0 or less would make code hang or other bad side effects.
  public static final int MIN_BUFFERED_RESPONSES_PER_STREAM = 1;
  public static final int MIN_STREAMS_PER_PARTITION = 1;
  private static final int DEFAULT_BIGQUERY_CLIENT_RETRIES = 10;
  private static final String ARROW_COMPRESSION_CODEC_OPTION = "arrowCompressionCodec";
  private static final String RESPONSE_COMPRESSION_CODEC_OPTION = "responseCompressionCodec";
  private static final WriteMethod DEFAULT_WRITE_METHOD = WriteMethod.INDIRECT;
  public static final int DEFAULT_CACHE_EXPIRATION_IN_MINUTES = 15;
  static final String BIGQUERY_JOB_LABEL_PREFIX = "bigQueryJobLabel.";
  static final String BIGQUERY_TABLE_LABEL_PREFIX = "bigQueryTableLabel.";
  public static final Priority DEFAULT_JOB_PRIORITY = Priority.INTERACTIVE;
  static final String ALLOW_MAP_TYPE_CONVERSION = "allowMapTypeConversion";
  static final Boolean ALLOW_MAP_TYPE_CONVERSION_DEFAULT = true;
  public static final String partitionOverwriteModeProperty =
      "spark.sql.sources.partitionOverwriteMode";

  public PartitionOverwriteMode partitionOverwriteModeValue = PartitionOverwriteMode.STATIC;
  public static final String BIGQUERY_JOB_TIMEOUT_IN_MINUTES = "bigQueryJobTimeoutInMinutes";
  static final long BIGQUERY_JOB_TIMEOUT_IN_MINUTES_DEFAULT = 6 * 60; // 6 hrs

  public static final String GPN_ATTRIBUTION = "GPN";

  public static final String BIG_NUMERIC_DEFAULT_PRECISION = "bigNumericDefaultPrecision";
  public static final String BIG_NUMERIC_DEFAULT_SCALE = "bigNumericDefaultScale";

  private static final String DATAPROC_SYSTEM_BUCKET_CONFIGURATION = "fs.gs.system.bucket";

  TableId tableId;
  // as the config needs to be Serializable, internally it uses
  // com.google.common.base.Optional<String> but externally it uses the regular java.util.Optional
  com.google.common.base.Optional<String> query = empty();
  String parentProjectId;
  boolean useParentProjectForMetadataOperations;
  com.google.common.base.Optional<String> accessTokenProviderFQCN;
  com.google.common.base.Optional<String> accessTokenProviderConfig;
  String loggedInUserName;
  Set<String> loggedInUserGroups;
  com.google.common.base.Optional<String> impersonationServiceAccount;
  com.google.common.base.Optional<Map<String, String>> impersonationServiceAccountsForUsers;
  com.google.common.base.Optional<Map<String, String>> impersonationServiceAccountsForGroups;
  com.google.common.base.Optional<String> credentialsKey;
  com.google.common.base.Optional<String> credentialsFile;
  com.google.common.base.Optional<String> accessToken;
  com.google.common.base.Optional<ImmutableList<String>> credentialsScopes;
  com.google.common.base.Optional<String> filter = empty();
  com.google.common.base.Optional<StructType> schema = empty();
  Integer maxParallelism = null;
  Integer preferredMinParallelism = null;
  int defaultParallelism = 1;
  com.google.common.base.Optional<String> temporaryGcsBucket = empty();
  com.google.common.base.Optional<String> persistentGcsBucket = empty();
  com.google.common.base.Optional<String> persistentGcsPath = empty();
  com.google.common.base.Optional<String> catalogProjectId = empty();
  com.google.common.base.Optional<String> catalogLocation = empty();

  IntermediateFormat intermediateFormat = DEFAULT_INTERMEDIATE_FORMAT;
  DataFormat readDataFormat = DEFAULT_READ_DATA_FORMAT;
  boolean combinePushedDownFilters = true;
  boolean viewsEnabled = false;
  com.google.common.base.Optional<String> materializationProject = empty();
  com.google.common.base.Optional<String> materializationDataset = empty();
  int materializationExpirationTimeInMinutes;
  com.google.common.base.Optional<String> partitionField = empty();
  Long partitionExpirationMs = null;
  com.google.common.base.Optional<Boolean> partitionRequireFilter = empty();
  com.google.common.base.Optional<TimePartitioning.Type> partitionType = empty();
  com.google.common.base.Optional<Long> partitionRangeStart = empty();
  com.google.common.base.Optional<Long> partitionRangeEnd = empty();
  com.google.common.base.Optional<Long> partitionRangeInterval = empty();
  com.google.common.base.Optional<ImmutableList<String>> clusteredFields = empty();
  com.google.common.base.Optional<JobInfo.CreateDisposition> createDisposition = empty();
  boolean optimizedEmptyProjection = true;
  boolean useAvroLogicalTypes = false;
  List<String> decimalTargetTypes = Collections.emptyList();
  List<JobInfo.SchemaUpdateOption> loadSchemaUpdateOptions = Collections.emptyList();
  int maxReadRowsRetries = 3;
  boolean pushAllFilters = true;
  boolean enableModeCheckForSchemaFields = true;
  private com.google.common.base.Optional<String> encodedCreateReadSessionRequest = empty();
  private com.google.common.base.Optional<String> bigQueryStorageGrpcEndpoint = empty();
  private com.google.common.base.Optional<String> bigQueryHttpEndpoint = empty();
  private int numBackgroundThreadsPerStream = 0;
  private int numPrebufferReadRowsResponses = MIN_BUFFERED_RESPONSES_PER_STREAM;
  private int numStreamsPerPartition = MIN_STREAMS_PER_PARTITION;
  private int channelPoolSize = 1;
  private com.google.common.base.Optional<Integer> flowControlWindowBytes =
      com.google.common.base.Optional.absent();
  private boolean enableReadSessionCaching = true;
  private long readSessionCacheDurationMins = 5L;
  private Long snapshotTimeMillis = null;
  private SparkBigQueryProxyAndHttpConfig sparkBigQueryProxyAndHttpConfig;
  private CompressionCodec arrowCompressionCodec = DEFAULT_ARROW_COMPRESSION_CODEC;
  private ResponseCompressionCodec responseCompressionCodec = DEFAULT_RESPONSE_COMPRESSION_CODEC;
  private WriteMethod writeMethod = DEFAULT_WRITE_METHOD;
  boolean writeAtLeastOnce = false;
  private int cacheExpirationTimeInMinutes = DEFAULT_CACHE_EXPIRATION_IN_MINUTES;
  // used to create BigQuery ReadSessions
  private com.google.common.base.Optional<String> traceId;
  private Map<String, String> bigQueryJobLabels = Collections.emptyMap();
  private Map<String, String> bigQueryTableLabels = Collections.emptyMap();
  private com.google.common.base.Optional<Long> createReadSessionTimeoutInSeconds;
  private QueryJobConfiguration.Priority queryJobPriority = DEFAULT_JOB_PRIORITY;

  private com.google.common.base.Optional<String> destinationTableKmsKeyName = empty();

  private boolean allowMapTypeConversion = ALLOW_MAP_TYPE_CONVERSION_DEFAULT;
  private long bigQueryJobTimeoutInMinutes = BIGQUERY_JOB_TIMEOUT_IN_MINUTES_DEFAULT;
  private com.google.common.base.Optional<String> gpn;
  private int bigNumericDefaultPrecision;
  private int bigNumericDefaultScale;
  private QueryParameterHelper queryParameterHelper;

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
    return from(
        options,
        customDefaults,
        dataSourceVersion,
        spark,
        schema,
        tableIsMandatory,
        Optional.empty());
  }

  public static SparkBigQueryConfig from(
      Map<String, String> options,
      ImmutableMap<String, String> customDefaults,
      DataSourceVersion dataSourceVersion,
      SparkSession spark,
      Optional<StructType> schema,
      boolean tableIsMandatory,
      Optional<TableId> overrideTableId) {
    Map<String, String> optionsMap = new HashMap<>(options);
    dataSourceVersion.updateOptionsMap(optionsMap);
    return SparkBigQueryConfig.from(
        ImmutableMap.copyOf(optionsMap),
        ImmutableMap.copyOf(scalaMapToJavaMap(spark.conf().getAll())),
        spark.sparkContext().hadoopConfiguration(),
        customDefaults,
        spark.sparkContext().defaultParallelism(),
        spark.sessionState().conf(),
        spark.version(),
        schema,
        tableIsMandatory,
        overrideTableId);
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
    return from(
        optionsInput,
        originalGlobalOptions,
        hadoopConfiguration,
        customDefaults,
        defaultParallelism,
        sqlConf,
        sparkVersion,
        schema,
        tableIsMandatory,
        Optional.empty());
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
      boolean tableIsMandatory,
      Optional<TableId> overrideTableId) {
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
    Optional<String> datasetParam = getOption(options, "dataset").or(fallbackDataset).toJavaUtil();
    Optional<String> projectParam =
        firstPresent(getOption(options, "project").toJavaUtil(), fallbackProject);
    config.partitionType =
        getOption(options, "partitionType").transform(TimePartitioning.Type::valueOf);
    config.partitionRangeStart =
        getOption(options, "partitionRangeStart").transform(Long::parseLong);
    config.partitionRangeEnd = getOption(options, "partitionRangeEnd").transform(Long::parseLong);
    config.partitionRangeInterval =
        getOption(options, "partitionRangeInterval").transform(Long::parseLong);
    if (overrideTableId.isPresent()) {
      config.tableId = overrideTableId.get();
    } else {
      // checking for query
      Optional<String> tableParam =
          getOptionFromMultipleParams(options, ImmutableList.of("table", "path"), DEFAULT_FALLBACK)
              .toJavaUtil();
      Optional<String> datePartitionParam = getOption(options, DATE_PARTITION_PARAM).toJavaUtil();
      datePartitionParam.ifPresent(
          date ->
              validateDateFormat(date, config.getPartitionTypeOrDefault(), DATE_PARTITION_PARAM));
      if (tableParam.isPresent()) {
        String tableParamStr = tableParam.get().trim();
        if (isQuery(tableParamStr)) {
          // it is a query in practice
          config.query = com.google.common.base.Optional.of(tableParamStr);
          config.tableId =
              datasetParam
                  .map(
                      ignored ->
                          parseTableId("QUERY", datasetParam, projectParam, datePartitionParam))
                  .orElse(QUERY_DUMMY_TABLE_ID);
        } else {
          config.tableId =
              parseTableId(tableParamStr, datasetParam, projectParam, datePartitionParam);
        }
      } else {
        // no table has been provided, it is either a query or an error
        config.query = getOption(options, "query").transform(String::trim);
        if (config.query.isPresent()) {
          config.tableId =
              datasetParam
                  .map(
                      ignored ->
                          parseTableId("QUERY", datasetParam, projectParam, datePartitionParam))
                  .orElse(QUERY_DUMMY_TABLE_ID);
        } else if (tableIsMandatory) {
          // No table nor query were set. We cannot go further.
          throw new IllegalArgumentException("No table has been specified");
        }
      }
    }

    config.parentProjectId =
        getAnyOption(globalOptions, options, "parentProject").or(defaultBilledProject());
    config.catalogProjectId = getOption(options, "projectId");
    config.catalogLocation = getOption(options, "bigquery_location");
    config.useParentProjectForMetadataOperations =
        getAnyBooleanOption(globalOptions, options, "useParentProjectForMetadataOperations", false);
    config.accessTokenProviderFQCN = getAnyOption(globalOptions, options, "gcpAccessTokenProvider");
    config.accessTokenProviderConfig =
        getAnyOption(globalOptions, options, "gcpAccessTokenProviderConfig");
    try {
      UserGroupInformation ugiCurrentUser = UserGroupInformation.getCurrentUser();
      config.loggedInUserName = ugiCurrentUser.getShortUserName();
      config.loggedInUserGroups = Sets.newHashSet(ugiCurrentUser.getGroupNames());
    } catch (IOException e) {
      throw new BigQueryConnectorException(
          "Failed to get the UserGroupInformation current user", e);
    }
    config.impersonationServiceAccount = getAnyOption(globalOptions, options, IMPERSONATION_GLOBAL);
    config.impersonationServiceAccountsForUsers =
        removePrefixFromMapKeys(
            getAnyOptionsWithPrefix(
                globalOptions, options, IMPERSONATION_FOR_USER_PREFIX.toLowerCase()),
            IMPERSONATION_FOR_USER_PREFIX.toLowerCase());
    config.impersonationServiceAccountsForGroups =
        removePrefixFromMapKeys(
            getAnyOptionsWithPrefix(
                globalOptions, options, IMPERSONATION_FOR_GROUP_PREFIX.toLowerCase()),
            IMPERSONATION_FOR_GROUP_PREFIX.toLowerCase());
    config.accessToken = getAnyOption(globalOptions, options, "gcpAccessToken");
    config.credentialsKey = getAnyOption(globalOptions, options, "credentials");
    config.credentialsFile =
        fromJavaUtil(
            firstPresent(
                getAnyOption(globalOptions, options, "credentialsFile").toJavaUtil(),
                com.google.common.base.Optional.fromNullable(
                        hadoopConfiguration.get(GCS_CONFIG_CREDENTIALS_FILE_PROPERTY))
                    .toJavaUtil()));
    config.credentialsScopes =
        getAnyOption(globalOptions, options, "credentialsScopes")
            .transform(SparkBigQueryConfig::splitOnComma);
    config.filter = getOption(options, "filter");
    config.schema = fromJavaUtil(schema);
    config.maxParallelism =
        getAnyOption(globalOptions, options, ImmutableList.of("maxParallelism", "parallelism"))
            .transform(Integer::valueOf)
            .orNull();
    config.preferredMinParallelism =
        getAnyOption(globalOptions, options, "preferredMinParallelism")
            .transform(Integer::valueOf)
            .orNull();
    config.defaultParallelism = defaultParallelism;
    config.temporaryGcsBucket =
        stripPrefix(getAnyOption(globalOptions, options, "temporaryGcsBucket"))
            .or(
                com.google.common.base.Optional.fromNullable(
                    hadoopConfiguration.get(DATAPROC_SYSTEM_BUCKET_CONFIGURATION)));
    config.persistentGcsBucket =
        stripPrefix(getAnyOption(globalOptions, options, "persistentGcsBucket"));
    config.persistentGcsPath = getOption(options, "persistentGcsPath");
    WriteMethod writeMethodDefault =
        Optional.ofNullable(customDefaults.get(WRITE_METHOD_PARAM))
            .map(WriteMethod::from)
            .orElse(DEFAULT_WRITE_METHOD);
    config.writeMethod =
        getAnyOption(globalOptions, options, WRITE_METHOD_PARAM)
            .transform(WriteMethod::from)
            .or(writeMethodDefault);
    config.writeAtLeastOnce =
        getAnyBooleanOption(globalOptions, options, WRITE_AT_LEAST_ONCE_OPTION, false);

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
    config.clusteredFields =
        getOption(options, "clusteredFields").transform(SparkBigQueryConfig::splitOnComma);

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
    config.loadSchemaUpdateOptions = Collections.unmodifiableList(loadSchemaUpdateOptions.build());
    config.decimalTargetTypes =
        getOption(options, "decimalTargetTypes")
            .transform(SparkBigQueryConfig::splitOnComma)
            .or(ImmutableList.of());
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
    config.flowControlWindowBytes =
        getAnyOption(globalOptions, options, "bqFlowControlWindowBytes")
            .transform(Integer::parseInt);

    config.numStreamsPerPartition =
        getAnyOption(globalOptions, options, "bqNumStreamsPerPartition")
            .transform(Integer::parseInt)
            .or(MIN_STREAMS_PER_PARTITION);
    // Calculating the default channel pool size
    int sparkExecutorCores =
        Integer.parseInt(globalOptions.getOrDefault("spark.executor.cores", "1"));
    int defaultChannelPoolSize = sparkExecutorCores * config.numStreamsPerPartition;

    config.channelPoolSize =
        getAnyOption(globalOptions, options, "bqChannelPoolSize")
            .transform(Integer::parseInt)
            .or(defaultChannelPoolSize);
    config.enableReadSessionCaching =
        getAnyBooleanOption(globalOptions, options, "enableReadSessionCaching", true);
    config.readSessionCacheDurationMins =
        getAnyOption(globalOptions, options, "readSessionCacheDurationMins")
            .transform(Long::parseLong)
            .or(5L);
    if (!(config.readSessionCacheDurationMins > 0L
        && config.readSessionCacheDurationMins <= 300L)) {
      throw new IllegalArgumentException("readSessionCacheDurationMins should be > 0 and <= 300");
    }

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

    String responseCompressionCodecParam =
        getAnyOption(globalOptions, options, RESPONSE_COMPRESSION_CODEC_OPTION)
            .transform(String::toUpperCase)
            .or(DEFAULT_RESPONSE_COMPRESSION_CODEC.toString());

    try {
      config.responseCompressionCodec =
          ResponseCompressionCodec.valueOf(responseCompressionCodecParam);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          format(
              "Response compression codec '%s' is not supported. Supported formats are %s",
              responseCompressionCodecParam, Arrays.toString(ResponseCompressionCodec.values())));
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
        getAnyOption(globalOptions, options, "traceApplicationName")
            .or(com.google.common.base.Optional.fromNullable("traceApplicationName"));
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

    config.queryJobPriority =
        getAnyOption(globalOptions, options, "queryJobPriority")
            .transform(String::toUpperCase)
            .transform(Priority::valueOf)
            .or(DEFAULT_JOB_PRIORITY);

    config.destinationTableKmsKeyName =
        getAnyOption(globalOptions, options, "destinationTableKmsKeyName");

    config.allowMapTypeConversion =
        getAnyOption(globalOptions, options, ALLOW_MAP_TYPE_CONVERSION)
            .transform(Boolean::valueOf)
            .or(ALLOW_MAP_TYPE_CONVERSION_DEFAULT);

    config.partitionOverwriteModeValue =
        getAnyOption(globalOptions, options, partitionOverwriteModeProperty)
            .transform(String::toUpperCase)
            .transform(PartitionOverwriteMode::valueOf)
            .or(PartitionOverwriteMode.STATIC);

    config.bigQueryJobTimeoutInMinutes =
        getAnyOption(globalOptions, options, BIGQUERY_JOB_TIMEOUT_IN_MINUTES)
            .transform(Long::valueOf)
            .or(BIGQUERY_JOB_TIMEOUT_IN_MINUTES_DEFAULT);

    config.gpn = getAnyOption(globalOptions, options, GPN_ATTRIBUTION);

    config.snapshotTimeMillis =
        getOption(options, "snapshotTimeMillis").transform(Long::valueOf).orNull();
    config.bigNumericDefaultPrecision =
        getAnyOption(globalOptions, options, BIG_NUMERIC_DEFAULT_PRECISION)
            .transform(Integer::parseInt)
            .or(BigQueryUtil.DEFAULT_BIG_NUMERIC_PRECISION);
    config.bigNumericDefaultScale =
        getAnyOption(globalOptions, options, BIG_NUMERIC_DEFAULT_SCALE)
            .transform(Integer::parseInt)
            .or(BigQueryUtil.DEFAULT_BIG_NUMERIC_SCALE);
    config.queryParameterHelper = BigQueryUtil.parseQueryParameters(options);
    return config;
  }

  private static ImmutableList<String> splitOnComma(String value) {
    return Splitter.on(",")
        .trimResults()
        .omitEmptyStrings()
        .splitToStream(value)
        .collect(ImmutableList.toImmutableList());
  }

  // strip gs:// prefix if exists
  private static com.google.common.base.Optional<String> stripPrefix(
      com.google.common.base.Optional<String> bucket) {
    return bucket.transform(
        path -> {
          if (path.startsWith("gs://")) {
            return path.substring(5);
          } else {
            return path;
          }
        });
  }

  @VisibleForTesting
  // takes only the options with the BIGQUERY_JOB_LABEL_PREFIX prefix, and strip them of this
  // prefix.
  // The `options` map overrides the `globalOptions` map.
  static Map<String, String> parseBigQueryLabels(
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

    return Collections.unmodifiableMap(result.build());
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
    if (tableParamStr == null || tableParamStr.trim().isEmpty()) {
      return false;
    }
    String potentialQuery = tableParamStr.trim();

    // If the string is quoted with backticks, it is recognized as a table identifier, not a query.
    if (potentialQuery.startsWith("`") && potentialQuery.endsWith("`")) {
      return false;
    }

    // Check for common query-starting keyword.
    if (QUICK_LOWERCASE_QUERY_PATTERN.matcher(potentialQuery).matches()) {
      return true;
    }

    // Might be a query with a leading comment, OR could be a table name with spaces.
    return HAS_WHITESPACE_PATTERN.matcher(potentialQuery).find()
        && SQL_KEYWORD_PATTERN.matcher(potentialQuery).find();
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
            loggedInUserName,
            loggedInUserGroups,
            impersonationServiceAccountsForUsers.toJavaUtil(),
            impersonationServiceAccountsForGroups.toJavaUtil(),
            impersonationServiceAccount.toJavaUtil(),
            credentialsScopes.toJavaUtil(),
            sparkBigQueryProxyAndHttpConfig.getProxyUri(),
            sparkBigQueryProxyAndHttpConfig.getProxyUsername(),
            sparkBigQueryProxyAndHttpConfig.getProxyPassword())
        .getCredentials();
  }

  public TableId getTableId() {
    return tableId;
  }

  /** Always populate the project id, even if it is null. */
  public TableId getTableIdWithExplicitProject() {
    if (tableId.getProject() != null) {
      return tableId;
    }
    return TableId.of(
        // if the table was not set, this is the used project
        ServiceOptions.getDefaultProjectId(), tableId.getDataset(), tableId.getTable());
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

  public QueryParameterHelper getQueryParameterHelper() {
    return queryParameterHelper;
  }

  @Override
  public String getParentProjectId() {
    return parentProjectId;
  }

  @Override
  public Optional<String> getCatalogProjectId() {
    return catalogProjectId.toJavaUtil();
  }

  @Override
  public Optional<String> getCatalogLocation() {
    return catalogLocation.toJavaUtil();
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
  public String getLoggedInUserName() {
    return loggedInUserName;
  }

  @Override
  public Set<String> getLoggedInUserGroups() {
    return loggedInUserGroups;
  }

  @Override
  public Optional<Map<String, String>> getImpersonationServiceAccountsForUsers() {
    return impersonationServiceAccountsForUsers.toJavaUtil();
  }

  @Override
  public Optional<Map<String, String>> getImpersonationServiceAccountsForGroups() {
    return impersonationServiceAccountsForGroups.toJavaUtil();
  }

  @Override
  public Optional<String> getImpersonationServiceAccount() {
    return impersonationServiceAccount.toJavaUtil();
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

  @Override
  public Optional<ImmutableList<String>> getCredentialsScopes() {
    return credentialsScopes.toJavaUtil();
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

  public ResponseCompressionCodec getResponseCompressionCodec() {
    return responseCompressionCodec;
  }

  public boolean isCombinePushedDownFilters() {
    return combinePushedDownFilters;
  }

  @Override
  public boolean isUseAvroLogicalTypes() {
    return useAvroLogicalTypes;
  }

  public ImmutableList<String> getDecimalTargetTypes() {
    return ImmutableList.copyOf(decimalTargetTypes);
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

  public int getMaterializationExpirationTimeInMinutes() {
    return materializationExpirationTimeInMinutes;
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

  public Optional<RangePartitioning.Range> getPartitionRange() {
    if (partitionRangeStart.isPresent()
        && partitionRangeEnd.isPresent()
        && partitionRangeInterval.isPresent()) {
      return Optional.of(
          RangePartitioning.Range.newBuilder()
              .setStart(partitionRangeStart.get())
              .setEnd(partitionRangeEnd.get())
              .setInterval(partitionRangeInterval.get())
              .build());
    }
    return Optional.empty();
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
    return ImmutableList.copyOf(loadSchemaUpdateOptions);
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

  public PartitionOverwriteMode getPartitionOverwriteModeValue() {
    return partitionOverwriteModeValue;
  }

  @Override
  public int getChannelPoolSize() {
    return channelPoolSize;
  }

  @Override
  public Optional<Integer> getFlowControlWindowBytes() {
    return flowControlWindowBytes.toJavaUtil();
  }

  @Override
  public Priority getQueryJobPriority() {
    return queryJobPriority;
  }

  @Override
  public Optional<String> getKmsKeyName() {
    return destinationTableKmsKeyName.toJavaUtil();
  }

  @Override
  public RetrySettings getBigQueryClientRetrySettings() {
    int maxAttempts =
        sparkBigQueryProxyAndHttpConfig.getHttpMaxRetry().orElse(DEFAULT_BIGQUERY_CLIENT_RETRIES);
    return getRetrySettings(maxAttempts);
  }

  private static RetrySettings getRetrySettings(int maxAttempts) {
    return RetrySettings.newBuilder()
        .setMaxAttempts(maxAttempts)
        .setTotalTimeout(Duration.ofMinutes(10))
        .setInitialRpcTimeout(Duration.ofSeconds(60))
        .setMaxRpcTimeout(Duration.ofMinutes(5))
        .setRpcTimeoutMultiplier(1.6)
        .setRetryDelayMultiplier(1.6)
        .setInitialRetryDelay(Duration.ofMillis(1250))
        .setMaxRetryDelay(Duration.ofSeconds(5))
        .build();
  }

  // for V2 write with BigQuery Storage Write API
  public RetrySettings getBigqueryDataWriteHelperRetrySettings() {
    return getRetrySettings(5);
  }

  public WriteMethod getWriteMethod() {
    return writeMethod;
  }

  public boolean isWriteAtLeastOnce() {
    return writeAtLeastOnce;
  }

  public Optional<String> getTraceId() {
    return traceId.toJavaUtil();
  }

  @Override
  public ImmutableMap<String, String> getBigQueryJobLabels() {
    return ImmutableMap.copyOf(bigQueryJobLabels);
  }

  public boolean getAllowMapTypeConversion() {
    return allowMapTypeConversion;
  }

  public long getBigQueryJobTimeoutInMinutes() {
    return bigQueryJobTimeoutInMinutes;
  }

  public ImmutableMap<String, String> getBigQueryTableLabels() {
    return ImmutableMap.copyOf(bigQueryTableLabels);
  }

  public Optional<String> getGpn() {
    return gpn.toJavaUtil();
  }

  public OptionalLong getSnapshotTimeMillis() {
    return snapshotTimeMillis == null ? OptionalLong.empty() : OptionalLong.of(snapshotTimeMillis);
  }

  public int getBigNumericDefaultPrecision() {
    return bigNumericDefaultPrecision;
  }

  public int getBigNumericDefaultScale() {
    return bigNumericDefaultScale;
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
        .setResponseCompressionCodec(responseCompressionCodec)
        .setTraceId(traceId.toJavaUtil())
        .setEnableReadSessionCaching(enableReadSessionCaching)
        .setReadSessionCacheDurationMins(readSessionCacheDurationMins)
        .setSnapshotTimeMillis(getSnapshotTimeMillis())
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
      public QueryParameterHelper getQueryParameterHelper() {
        return SparkBigQueryConfig.this.getQueryParameterHelper();
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
