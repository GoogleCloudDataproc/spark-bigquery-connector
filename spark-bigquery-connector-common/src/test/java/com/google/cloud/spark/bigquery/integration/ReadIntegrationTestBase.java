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

import static com.google.cloud.spark.bigquery.integration.TestConstants.ALL_TYPES_TABLE_SCHEMA;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.connector.common.integration.DefaultCredentialsDelegateAccessTokenProvider;
import com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ReadIntegrationTestBase extends SparkBigQueryIntegrationTestBaseV2 {

  protected SparkBigQueryIntegrationTestRunner testRunner =
      new InMemorySparkBigQueryIntegrationTestRunner();

  private static final TimeZone DEFAULT_TZ = TimeZone.getDefault();
  private static final Map<String, Collection<String>> FILTER_DATA =
      ImmutableMap.<String, Collection<String>>builder()
          .put("word_count == 4", ImmutableList.of("'A", "'But", "'Faith"))
          .put("word_count > 3", ImmutableList.of("'", "''Tis", "'A"))
          .put("word_count >= 2", ImmutableList.of("'", "''Lo", "''O"))
          .put("word_count < 3", ImmutableList.of("''All", "''Among", "''And"))
          .put("word_count <= 5", ImmutableList.of("'", "''All", "''Among"))
          .put("word_count in(8, 9)", ImmutableList.of("'", "'Faith", "'Tis"))
          .put("word_count is null", ImmutableList.of())
          .put("word_count is not null", ImmutableList.of("'", "''All", "''Among"))
          .put(
              "word_count == 4 and corpus == 'twelfthnight'",
              ImmutableList.of("'Thou", "'em", "Art"))
          .put(
              "word_count == 4 or corpus > 'twelfthnight'",
              ImmutableList.of("'", "''Tis", "''twas"))
          .put("not word_count in(8, 9)", ImmutableList.of("'", "''All", "''Among"))
          .put("corpus like 'king%'", ImmutableList.of("'", "'A", "'Affectionate"))
          .put("corpus like '%kinghenryiv'", ImmutableList.of("'", "'And", "'Anon"))
          .put("corpus like '%king%'", ImmutableList.of("'", "'A", "'Affectionate"))
          .build();
  protected final String PROJECT_ID =
      Preconditions.checkNotNull(
          System.getenv("GOOGLE_CLOUD_PROJECT"),
          "Please set the GOOGLE_CLOUD_PROJECT env variable in order to read views");

  private static final StructType SHAKESPEARE_TABLE_SCHEMA_WITH_METADATA_COMMENT =
      new StructType(
          Stream.of(TestConstants.SHAKESPEARE_TABLE_SCHEMA.fields())
              .map(
                  field -> {
                    Metadata metadata =
                        new MetadataBuilder()
                            .withMetadata(field.metadata())
                            .putString("comment", field.metadata().getString("description"))
                            .build();
                    return new StructField(
                        field.name(), field.dataType(), field.nullable(), metadata);
                  })
              .toArray(StructField[]::new));

  private static final String LARGE_TABLE = "bigquery-public-data.samples.natality";
  private static final String LARGE_TABLE_FIELD = "is_male";
  private static final long LARGE_TABLE_NUM_ROWS = 33271914L;
  private static final String NON_EXISTENT_TABLE = "non-existent.non-existent.non-existent";
  private static final String ALL_TYPES_TABLE_NAME = "all_types";

  protected StructType allTypesTableSchema;

  protected final boolean userProvidedSchemaAllowed;
  protected Optional<DataType> timeStampNTZType = Optional.empty();

  protected List<String> gcsObjectsToClean = new ArrayList<>();

  BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

  public ReadIntegrationTestBase() {
    this(true, Optional.empty());
  }

  public ReadIntegrationTestBase(boolean userProvidedSchemaAllowed) {
    this(userProvidedSchemaAllowed, Optional.empty());
  }

  public ReadIntegrationTestBase(boolean userProvidedSchemaAllowed, DataType timestampNTZType) {
    this(userProvidedSchemaAllowed, Optional.of(timestampNTZType));
  }

  public ReadIntegrationTestBase(
      boolean userProvidedSchemaAllowed, Optional<DataType> timeStampNTZType) {
    super();
    this.userProvidedSchemaAllowed = userProvidedSchemaAllowed;
    this.timeStampNTZType = timeStampNTZType;
    intializeSchema(timeStampNTZType);
  }

  private void intializeSchema(Optional<DataType> timeStampNTZType) {
    if (!timeStampNTZType.isPresent()) {
      allTypesTableSchema = ALL_TYPES_TABLE_SCHEMA;
      return;
    }
    allTypesTableSchema = new StructType();
    for (StructField field : ALL_TYPES_TABLE_SCHEMA.fields()) {
      DataType dateTimeType = field.name().equals("dt") ? timeStampNTZType.get() : field.dataType();
      allTypesTableSchema =
          allTypesTableSchema.add(field.name(), dateTimeType, field.nullable(), field.metadata());
    }
  }

  public static JsonObject convertSchemaToJsonObject(StructType schema) {
    return JsonParser.parseString(schema.json()).getAsJsonObject();
  }

  @Before
  public void clearGcsObjectsToCleanList() {
    gcsObjectsToClean.clear();
  }

  @After
  public void cleanGcsObjects() throws Exception {
    for (String object : gcsObjectsToClean) {
      AcceptanceTestUtils.deleteGcsDir(object);
    }
  }

  @After
  public void resetDefaultTimeZone() {
    TimeZone.setDefault(DEFAULT_TZ);
  }

  // =========================================================================
  // STATIC METHOD FUNCTIONAL APPLICATIONS MAPPINGS
  // =========================================================================

  @SuppressWarnings("resource")
  protected static JsonObject readShakespeareApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    String scenario = parameters.getOrDefault("scenario", "OPTION");
    String table = parameters.getOrDefault("table", TestConstants.SHAKESPEARE_TABLE);
    String bqEncodedRequest = parameters.get("bqEncodedCreateReadSessionRequest");
    String backgroundThreads = parameters.get("bqBackgroundThreadsPerStream");

    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("readShakespeareApp")
            .getOrCreate()
            .newSession();
    org.apache.spark.sql.DataFrameReader reader = spark.read().format("bigquery");

    if (bqEncodedRequest != null) {
      reader = reader.option("bqEncodedCreateReadSessionRequest", bqEncodedRequest);
    }
    if (backgroundThreads != null) {
      reader = reader.option("bqBackgroundThreadsPerStream", backgroundThreads);
    }

    Dataset<Row> df = null;
    if ("SIMPLIFIED".equals(scenario)) {
      df = reader.load(table);
    } else {
      df = reader.option("table", table).load();
    }

    if (bqEncodedRequest != null) {
      df.head();
    }

    JsonObject result = generateShakespeareTestValues(df);
    result.addProperty("status", "success");
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readSchemaPrunedApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset)
            .option("table", ALL_TYPES_TABLE_NAME)
            .load();

    List<Row> rows =
        df.select("str", "nested_struct.str", "nested_struct.inner_struct.str").collectAsList();

    JsonObject result = new JsonObject();
    if (rows.isEmpty()) {
      result.addProperty("status", "failed");
      result.addProperty("error", "No rows returned by query");
      return result;
    }
    Row res = rows.get(0);
    result.addProperty("status", "success");
    result.addProperty("strVal", res.getString(0));
    result.addProperty("nestedStrVal", res.getString(1));
    result.addProperty("innerStrVal", res.getString(2));
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readFilteredShakespeareApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    String scenario = parameters.getOrDefault("scenario", "FILTERS");
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");

    if ("COUNT_WITH_FILTERS".equals(scenario)) {
      long countResults =
          spark
              .read()
              .format("bigquery")
              .option("table", "bigquery-public-data.samples.shakespeare")
              .option("readDataFormat", "ARROW")
              .load()
              .where("word_count = 1 OR corpus_date = 0")
              .count();
      long countAfterCollect =
          spark
              .read()
              .format("bigquery")
              .option("table", "bigquery-public-data.samples.shakespeare")
              .option("readDataFormat", "ARROW")
              .load()
              .where("word_count = 1 OR corpus_date = 0")
              .collectAsList()
              .size();
      result.addProperty("countResults", countResults);
      result.addProperty("countAfterCollect", countAfterCollect);
    } else {
      Dataset<Row> df = spark.read().format("bigquery").load(TestConstants.SHAKESPEARE_TABLE);
      result.addProperty("count", df.count());
      boolean filtersCorrect = true;
      for (Map.Entry<String, Collection<String>> entry : FILTER_DATA.entrySet()) {
        List<String> firstWords =
            Arrays.asList(
                (String[])
                    df.select("word")
                        .where(entry.getKey())
                        .distinct()
                        .as(Encoders.STRING())
                        .sort("word")
                        .take(3));
        if (!firstWords.containsAll(entry.getValue())) {
          filtersCorrect = false;
          break;
        }
      }
      result.addProperty("filtersCorrect", filtersCorrect);
    }
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readSchemaMetadataApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    String scenario = parameters.getOrDefault("scenario", "SCHEMA");
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");

    if ("NON_EXISTENT".equals(scenario)) {
      spark.read().format("bigquery").option("table", NON_EXISTENT_TABLE).load();
    } else if ("USER_DEFINED".equals(scenario)) {
      StructType expectedSchema =
          new StructType(
              new StructField[] {
                new StructField("whatever", DataTypes.ByteType, true, Metadata.empty())
              });
      Dataset<Row> table =
          spark
              .read()
              .schema(expectedSchema)
              .format("bigquery")
              .option("table", TestConstants.SHAKESPEARE_TABLE)
              .load();
      result.add("resultSchema", convertSchemaToJsonObject(table.schema()));
    } else {
      Dataset<Row> df =
          spark
              .read()
              .format("bigquery")
              .option("dataset", testDataset)
              .option("table", ALL_TYPES_TABLE_NAME)
              .load();
      result.addProperty(
          "sizeInBytes", df.queryExecution().analyzed().stats().sizeInBytes().longValue());
      result.add("resultSchema", convertSchemaToJsonObject(df.schema()));
    }
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readHeadTimeoutApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();
    spark
        .read()
        .format("bigquery")
        .option("table", LARGE_TABLE)
        .load()
        .select(LARGE_TABLE_FIELD)
        .head();
    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readUnhandledFilterStructApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data:samples.github_nested")
            .option("filter", "url like '%spark'")
            .load();

    List<Row> rows = df.select("url").where("repository is not null").collectAsList();
    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    result.addProperty("rowCount", rows.size());
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readMaterializedViewApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    String scenario = parameters.getOrDefault("scenario", "WITH_MATERIALIZATION");
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();
    Dataset<Row> df = null;

    if ("NO_MATERIALIZATION".equals(scenario)) {
      df =
          spark
              .read()
              .format("bigquery")
              .option("dataset", testDataset)
              .option("table", TestConstants.SHAKESPEARE_VIEW)
              .option("viewsEnabled", "true")
              .load();
    } else {
      String proj = parameters.get("viewMaterializationProject");
      df =
          spark
              .read()
              .format("bigquery")
              .option("table", "bigquery-public-data:bigqueryml_ncaa.cume_games_view")
              .option("viewsEnabled", "true")
              .option("viewMaterializationProject", proj)
              .option("viewMaterializationDataset", testDataset)
              .load();
    }

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    result.addProperty("count", df.count());
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readCompressedCodecsApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    String scenario = parameters.getOrDefault("scenario", "OR_FILTER");
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();
    JsonObject result = new JsonObject();
    result.addProperty("status", "success");

    if ("ARROW_COMPRESSION".equals(scenario)) {
      List<Row> avroResults =
          spark
              .read()
              .format("bigquery")
              .option("table", "bigquery-public-data.samples.shakespeare")
              .option("filter", "word_count = 1 OR corpus_date = 0")
              .option("readDataFormat", "AVRO")
              .load()
              .sort("word", "word_count", "corpus", "corpus_date")
              .collectAsList();
      String arrowCodec = parameters.get("arrowCompressionCodec");
      List<Row> arrowCodecResults =
          spark
              .read()
              .format("bigquery")
              .option("table", "bigquery-public-data.samples.shakespeare")
              .option("readDataFormat", "ARROW")
              .option("arrowCompressionCodec", arrowCodec)
              .load()
              .where("word_count = 1 OR corpus_date = 0")
              .sort("word", "word_count", "corpus", "corpus_date")
              .collectAsList();
      result.addProperty("matches", avroResults.equals(arrowCodecResults));
    } else if ("RESPONSE_COMPRESSION".equals(scenario)) {
      String format = parameters.get("readDataFormat");
      List<Row> arrowResultsUncompressed =
          spark
              .read()
              .format("bigquery")
              .option("table", "bigquery-public-data.samples.shakespeare")
              .option("readDataFormat", "ARROW")
              .load()
              .where("word_count = 1 OR corpus_date = 0")
              .sort("word", "word_count", "corpus", "corpus_date")
              .collectAsList();
      List<Row> formatCodecResults =
          spark
              .read()
              .format("bigquery")
              .option("table", "bigquery-public-data.samples.shakespeare")
              .option("readDataFormat", format)
              .option("responseCompressionCodec", "RESPONSE_COMPRESSION_CODEC_LZ4")
              .load()
              .where("word_count = 1 OR corpus_date = 0")
              .sort("word", "word_count", "corpus", "corpus_date")
              .collectAsList();
      result.addProperty("matches", arrowResultsUncompressed.equals(formatCodecResults));
    } else {
      List<Row> avroResults =
          spark
              .read()
              .format("bigquery")
              .option("table", "bigquery-public-data.samples.shakespeare")
              .option("filter", "word_count = 1 OR corpus_date = 0")
              .option("readDataFormat", "AVRO")
              .load()
              .sort("word", "word_count", "corpus", "corpus_date")
              .collectAsList();
      List<Row> arrowResults =
          spark
              .read()
              .format("bigquery")
              .option("table", "bigquery-public-data.samples.shakespeare")
              .option("readDataFormat", "ARROW")
              .load()
              .where("word_count = 1 OR corpus_date = 0")
              .sort("word", "word_count", "corpus", "corpus_date")
              .collectAsList();
      result.addProperty("matches", avroResults.equals(arrowResults));
    }
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readBigLakeTableApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset)
            .option("table", testTable)
            .load();

    JsonObject result = generateShakespeareTestValues(df);
    result.addProperty("status", "success");
    return result;
  }

  protected static JsonObject generateShakespeareTestValues(Dataset<Row> df) {
    JsonObject result = new JsonObject();
    result.addProperty("count", df.count());
    result.add("resultSchema", convertSchemaToJsonObject(df.schema()));
    JsonArray firstWords = new JsonArray(3);
    Arrays.asList(
            (String[])
                df.select("word")
                    .where("word >= 'a' AND word not like '%\\'%'")
                    .distinct()
                    .as(Encoders.STRING())
                    .sort("word")
                    .take(3))
        .forEach(firstWords::add);
    result.add("firstWords", firstWords);
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readTableSnapshotApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    String snapshot = parameters.get("snapshot");
    String allTypes = parameters.get("allTypes");
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();

    Row[] allTypesRows =
        (Row[])
            spark
                .read()
                .format("bigquery")
                .option("dataset", testDataset)
                .option("table", allTypes)
                .load()
                .collect();
    Row[] snapshotRows =
        (Row[])
            spark
                .read()
                .format("bigquery")
                .option("dataset", testDataset)
                .option("table", snapshot)
                .load()
                .collect();

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    result.addProperty("matches", Arrays.equals(allTypesRows, snapshotRows));
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readTableWithSpacesApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    String tableId = parameters.get("tableId");
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();

    Dataset<Row> df = spark.read().format("bigquery").load(tableId);
    Dataset<Row> filteredDf =
        spark.read().format("bigquery").option("filter", "id > 1").load(tableId);
    List<Row> rows = filteredDf.sort("id").collectAsList();

    JsonObject result = new JsonObject();
    if (rows.isEmpty()) {
      result.addProperty("status", "failed");
      result.addProperty("error", "No rows returned by filtered query");
      return result;
    }
    result.addProperty("status", "success");
    result.addProperty("count", df.count());
    result.addProperty("filteredCount", filteredDf.count());
    result.addProperty("sortedFirstId", rows.get(0).getLong(0));
    result.addProperty("sortedFirstData", rows.get(0).getString(1));
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readSessionTimeoutApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    String scenario = parameters.getOrDefault("scenario", "HIGH_TIMEOUT");
    String table = parameters.get("table");
    int timeout = Integer.parseInt(parameters.get("timeout"));
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();

    org.apache.spark.sql.DataFrameReader reader =
        spark
            .read()
            .format("bigquery")
            .option("table", table)
            .option("createReadSessionTimeoutInSeconds", timeout);

    Dataset<Row> df = null;
    if ("LOW_TIMEOUT".equals(scenario)) {
      df = reader.option("preferredMinParallelism", "9000").load();
      df.where(
              "views>1000 AND title='Google' AND DATE(datehour) BETWEEN DATE('2021-01-01') AND DATE('2021-07-01')")
          .collect();
    } else {
      df = reader.load();
      df.collectAsList();
    }

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readNestedFieldProjectionApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();
    Dataset<Row> githubNestedDF =
        spark.read().format("bigquery").load("bigquery-public-data:samples.github_nested");
    List<Row> repositoryUrlRows =
        githubNestedDF
            .filter(
                "repository.has_downloads = true AND url = 'https://github.com/googleapi/googleapi'")
            .select("repository.url")
            .collectAsList();

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    result.addProperty("rowCount", repositoryUrlRows.size());
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readFilteredTimestampApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();
    String originalSessionTz = spark.conf().get("spark.sql.session.timeZone");
    try {
      spark.conf().set("spark.sql.session.timeZone", "UTC");
      Dataset<Row> df =
          spark
              .read()
              .format("bigquery")
              .option("dataset", testDataset)
              .option("table", testTable)
              .load();
      Dataset<Row> filteredDF =
          df.where(df.apply("eventTime").between("2023-01-09 10:00:00", "2023-01-09 10:00:00"));
      List<Row> resultList = filteredDF.collectAsList();

      JsonObject result = new JsonObject();
      if (resultList.isEmpty()) {
        result.addProperty("status", "failed");
        result.addProperty("error", "No rows returned by filtered eventTime query");
        return result;
      }
      result.addProperty("status", "success");
      result.addProperty("rowCount", resultList.size());
      Row head = resultList.get(0);
      result.addProperty(
          "eventTimeMillis", ((Timestamp) head.get(head.fieldIndex("eventTime"))).getTime());
      return result;
    } finally {
      spark.conf().set("spark.sql.session.timeZone", originalSessionTz);
    }
  }

  @SuppressWarnings("resource")
  protected static JsonObject readArrowTimestampRebaseApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();
    String originalSessionTz = spark.conf().get("spark.sql.session.timeZone");
    try {
      spark.conf().set("spark.sql.session.timeZone", "UTC");
      List<Row> rowsWithoutRebase =
          spark
              .read()
              .format("bigquery")
              .option("dataset", testDataset)
              .option("table", testTable)
              .option("readDataFormat", "ARROW")
              .option("enableArrowTimestampRebase", "false")
              .load()
              .collectAsList();
      List<Row> rowsWithRebase =
          spark
              .read()
              .format("bigquery")
              .option("dataset", testDataset)
              .option("table", testTable)
              .option("readDataFormat", "ARROW")
              .option("enableArrowTimestampRebase", "true")
              .load()
              .collectAsList();

      JsonObject result = new JsonObject();
      if (rowsWithoutRebase.isEmpty() || rowsWithRebase.isEmpty()) {
        result.addProperty("status", "failed");
        result.addProperty("error", "No rows returned by query");
        return result;
      }
      Row withoutRebase = rowsWithoutRebase.get(0);
      Row withRebase = rowsWithRebase.get(0);

      result.addProperty("status", "success");
      result.addProperty(
          "withoutRebaseMillis",
          ((Timestamp) withoutRebase.get(withoutRebase.fieldIndex("ts"))).getTime());
      result.addProperty(
          "withRebaseMillis", ((Timestamp) withRebase.get(withRebase.fieldIndex("ts"))).getTime());
      return result;
    } finally {
      spark.conf().set("spark.sql.session.timeZone", originalSessionTz);
    }
  }

  @SuppressWarnings("resource")
  protected static JsonObject readPushDateTimePredicateApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset)
            .option("table", testTable)
            .load()
            .where("orderDateTime < '2023-10-25 10:00:00'");

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    result.addProperty("count", df.count());
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readPseudoColumnsRuntimeFilteringApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    String scenario = parameters.get("scenario");
    String filter = parameters.get("filter");
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadIntegrationTestApp")
            .getOrCreate()
            .newSession();

    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset)
            .option("table", testTable)
            .option("filter", filter)
            .load()
            .select("orderId");
    List<Row> rows = df.join(df, "orderId").orderBy("orderId").collectAsList();

    JsonObject result = new JsonObject();
    if (rows.size() < 2) {
      result.addProperty("status", "failed");
      result.addProperty("error", "Fewer than 2 rows returned by query (got " + rows.size() + ")");
      return result;
    }
    result.addProperty("status", "success");
    result.addProperty("rowCount", rows.size());
    result.addProperty("firstId", rows.get(0).getLong(0));
    result.addProperty("secondId", rows.get(1).getLong(0));
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readExecuteCommandApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    String query = parameters.get("query");
    // @SuppressWarnings("resource") is required because this helper uses newSession().
    // In-process Spark tests are designed to be isolated; closing a newSession()
    // will break session context for subsequent tests. Do not close this session.
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("readExecuteCommandApp")
            .getOrCreate()
            .newSession();
    scala.collection.immutable.Map emptyScalaMap = scala.collection.immutable.Map$.MODULE$.empty();

    java.lang.reflect.Method executeCommandMethod =
        spark
            .getClass()
            .getMethod(
                "executeCommand", String.class, String.class, scala.collection.immutable.Map.class);
    @SuppressWarnings("unchecked")
    Dataset<Row> output =
        (Dataset<Row>) executeCommandMethod.invoke(spark, "bigquery", query, emptyScalaMap);

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    result.addProperty("count", output.count());
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readDataTypesApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("readDataTypesApp")
            .getOrCreate()
            .newSession();
    Dataset<Row> allTypesTable =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset)
            .option("table", ALL_TYPES_TABLE_NAME)
            .load();
    boolean isTimestampNTZType =
        Boolean.parseBoolean(parameters.getOrDefault("timestamp_ntz_type", "false"));
    List<org.apache.spark.sql.Column> cols = new ArrayList<>(TestConstants.ALL_TYPES_TABLE_COLS);
    if (isTimestampNTZType) {
      cols.set(
          6,
          org.apache.spark.sql.functions.lit("2019-03-18T01:23:45.678901").cast("timestamp_ntz"));
    }
    Row expectedValues =
        spark.range(1).select(cols.toArray(new org.apache.spark.sql.Column[0])).head();
    Row row = allTypesTable.head();
    IntegrationTestUtils.compareRows(row, expectedValues);

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    return result;
  }

  @SuppressWarnings("resource")
  protected static JsonObject readCustomAccessTokenProviderApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("readCustomAccessTokenProviderApp")
            .getOrCreate()
            .newSession();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option(
                "gcpAccessTokenProvider",
                DefaultCredentialsDelegateAccessTokenProvider.class.getCanonicalName())
            .load(TestConstants.SHAKESPEARE_TABLE);

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    result.addProperty("count", df.count());
    return result;
  }

  // =========================================================================
  // JUNIT TEST CASES DELEGATION MAPPINGS
  // =========================================================================

  private void testShakespeareLocal(JsonObject result) {
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("count").getAsLong()).isEqualTo(TestConstants.SHAKESPEARE_TABLE_NUM_ROWS);
    assertThat(result.get("resultSchema").getAsJsonObject())
        .isEqualTo(convertSchemaToJsonObject(SHAKESPEARE_TABLE_SCHEMA_WITH_METADATA_COMMENT));
    List<String> firstWords =
        StreamSupport.stream(result.get("firstWords").getAsJsonArray().spliterator(), false)
            .map(JsonElement::getAsString)
            .collect(Collectors.toList());
    assertThat(firstWords).hasSize(3);
    assertThat(firstWords).containsExactly("a", "abaissiez", "abandon");
  }

  @Test
  public void testReadWithOption() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readShakespeareApp,
            "",
            "",
            ImmutableMap.of("scenario", "OPTION", "table", TestConstants.SHAKESPEARE_TABLE));
    testShakespeareLocal(result);
  }

  @Test
  public void testReadWithSimplifiedApi() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readShakespeareApp,
            "",
            "",
            ImmutableMap.of("scenario", "SIMPLIFIED", "table", TestConstants.SHAKESPEARE_TABLE));
    testShakespeareLocal(result);
  }

  @Test
  @Ignore("DSv2 only")
  public void testReadCompressed() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readShakespeareApp,
            "",
            "",
            ImmutableMap.of(
                "scenario", "OPTION",
                "table", TestConstants.SHAKESPEARE_TABLE,
                "bqEncodedCreateReadSessionRequest", "EgZCBBoCEAI="));
    testShakespeareLocal(result);
  }

  @Test
  @Ignore("DSv2 only")
  public void testReadCompressedWith1BackgroundThreads() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readShakespeareApp,
            "",
            "",
            ImmutableMap.of(
                "scenario", "OPTION",
                "table", TestConstants.SHAKESPEARE_TABLE,
                "bqEncodedCreateReadSessionRequest", "EgZCBBoCEAI=",
                "bqBackgroundThreadsPerStream", "1"));
    testShakespeareLocal(result);
  }

  @Test
  @Ignore("DSv2 only")
  public void testReadCompressedWith4BackgroundThreads() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readShakespeareApp,
            "",
            "",
            ImmutableMap.of(
                "scenario", "OPTION",
                "table", TestConstants.SHAKESPEARE_TABLE,
                "bqEncodedCreateReadSessionRequest", "EgZCBBoCEAI=",
                "bqBackgroundThreadsPerStream", "4"));
    testShakespeareLocal(result);
  }

  @Test
  public void testReadSchemaPruned() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readSchemaPrunedApp,
            testDataset.toString(),
            "",
            ImmutableMap.of());
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("strVal").getAsString()).isEqualTo("string");
    assertThat(result.get("nestedStrVal").getAsString()).isEqualTo("stringa");
    assertThat(result.get("innerStrVal").getAsString()).isEqualTo("stringaa");
  }

  @Test
  public void testFilters() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readFilteredShakespeareApp,
            "",
            "",
            ImmutableMap.of("scenario", "FILTERS"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("count").getAsLong()).isEqualTo(TestConstants.SHAKESPEARE_TABLE_NUM_ROWS);
    assertThat(result.get("filtersCorrect").getAsBoolean()).isTrue();
  }

  @Test
  public void testCountWithFilters() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readFilteredShakespeareApp,
            "",
            "",
            ImmutableMap.of("scenario", "COUNT_WITH_FILTERS"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("countResults").getAsLong())
        .isEqualTo(result.get("countAfterCollect").getAsLong());
  }

  @Test
  public void testKnownSizeInBytes() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readSchemaMetadataApp,
            testDataset.toString(),
            "",
            ImmutableMap.of("scenario", "SCHEMA"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("sizeInBytes").getAsLong()).isEqualTo(TestConstants.ALL_TYPES_TABLE_SIZE);
  }

  @Test
  public void testKnownSchema() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readSchemaMetadataApp,
            testDataset.toString(),
            "",
            ImmutableMap.of("scenario", "SCHEMA"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("resultSchema").getAsJsonObject())
        .isEqualTo(convertSchemaToJsonObject(allTypesTableSchema));
  }

  @Test
  public void testUserDefinedSchema() throws Exception {
    assumeTrue("user provided schema is not allowed for this connector", userProvidedSchemaAllowed);
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readSchemaMetadataApp,
            "",
            "",
            ImmutableMap.of("scenario", "USER_DEFINED"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    StructType expectedSchema =
        new StructType(
            new StructField[] {
              new StructField("whatever", DataTypes.ByteType, true, Metadata.empty())
            });
    assertThat(result.get("resultSchema").getAsJsonObject())
        .isEqualTo(convertSchemaToJsonObject(expectedSchema));
  }

  @Test
  public void testNonExistentSchema() {
    assertThrows(
        "Trying to read a non existing table should throw an exception",
        RuntimeException.class,
        () -> {
          testRunner.run(
              ReadIntegrationTestBase::readSchemaMetadataApp,
              "",
              "",
              ImmutableMap.of("scenario", "NON_EXISTENT"));
        });
  }

  @Test(timeout = 10_000) // 10 seconds
  public void testHeadDoesNotTimeoutAndOOM() throws Exception {
    JsonObject result =
        testRunner.run(ReadIntegrationTestBase::readHeadTimeoutApp, "", "", ImmutableMap.of());
    assertThat(result.get("status").getAsString()).isEqualTo("success");
  }

  @Test
  public void testUnhandledFilterOnStruct() throws Exception {
    Assume.assumeThat(
        org.apache.spark.package$.MODULE$.SPARK_VERSION(), CoreMatchers.startsWith("3."));
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readUnhandledFilterStructApp, "", "", ImmutableMap.of());
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("rowCount").getAsInt()).isEqualTo(85);
  }

  @Test
  public void testQueryMaterializedView() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readMaterializedViewApp,
            testDataset.toString(),
            "",
            ImmutableMap.of(
                "scenario", "WITH_MATERIALIZATION", "viewMaterializationProject", PROJECT_ID));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("count").getAsLong()).isGreaterThan(1L);
  }

  @Test
  public void testQueryMaterializedView_noMaterializationDataset() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readMaterializedViewApp,
            testDataset.toString(),
            "",
            ImmutableMap.of("scenario", "NO_MATERIALIZATION"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("count").getAsLong()).isGreaterThan(1L);
  }

  @Test
  public void testOrAcrossColumnsAndFormats() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readCompressedCodecsApp,
            "",
            "",
            ImmutableMap.of("scenario", "OR_FILTER"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("matches").getAsBoolean()).isTrue();
  }

  @Test
  public void testArrowResponseCompressionCodec() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readCompressedCodecsApp,
            "",
            "",
            ImmutableMap.of("scenario", "RESPONSE_COMPRESSION", "readDataFormat", "ARROW"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("matches").getAsBoolean()).isTrue();
  }

  @Test
  public void testAvroResponseCompressionCodec() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readCompressedCodecsApp,
            "",
            "",
            ImmutableMap.of("scenario", "RESPONSE_COMPRESSION", "readDataFormat", "AVRO"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("matches").getAsBoolean()).isTrue();
  }

  @Test
  public void testArrowCompressionCodec() throws Exception {
    JsonObject result1 =
        testRunner.run(
            ReadIntegrationTestBase::readCompressedCodecsApp,
            "",
            "",
            ImmutableMap.of("scenario", "ARROW_COMPRESSION", "arrowCompressionCodec", "ZSTD"));
    assertThat(result1.get("status").getAsString()).isEqualTo("success");
    assertThat(result1.get("matches").getAsBoolean()).isTrue();

    JsonObject result2 =
        testRunner.run(
            ReadIntegrationTestBase::readCompressedCodecsApp,
            "",
            "",
            ImmutableMap.of("scenario", "ARROW_COMPRESSION", "arrowCompressionCodec", "LZ4_FRAME"));
    assertThat(result2.get("status").getAsString()).isEqualTo("success");
    assertThat(result2.get("matches").getAsBoolean()).isTrue();
  }

  @Test
  public void testReadFromBigLakeTable_csv() throws Exception {
    JsonObject result =
        testBigLakeTable(FormatOptions.csv(), TestConstants.SHAKESPEARE_CSV_FILENAME, "text/csv");
    testShakespeareLocal(result);
  }

  @Test
  public void testReadFromBigLakeTable_json() throws Exception {
    JsonObject result =
        testBigLakeTable(
            FormatOptions.json(), TestConstants.SHAKESPEARE_JSON_FILENAME, "application/json");
    testShakespeareLocal(result);
  }

  @Test
  public void testReadFromBigLakeTable_parquet() throws Exception {
    JsonObject result =
        testBigLakeTable(
            FormatOptions.parquet(),
            TestConstants.SHAKESPEARE_PARQUET_FILENAME,
            "application/octet-stream");
    testShakespeareLocal(result);
  }

  @Test
  public void testReadFromBigLakeTable_avro() throws Exception {
    JsonObject result =
        testBigLakeTable(
            FormatOptions.avro(),
            TestConstants.SHAKESPEARE_AVRO_FILENAME,
            "application/octet-stream");
    testShakespeareLocal(result);
  }

  private void uploadFileToGCS(String resourceName, String destinationURI, String contentType) {
    try {
      AcceptanceTestUtils.uploadToGcs(
          getClass().getResourceAsStream("/integration/" + resourceName),
          destinationURI,
          contentType);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private JsonObject testBigLakeTable(
      FormatOptions formatOptions, String dataFileName, String mimeType) throws Exception {
    String table = testTable + "_" + formatOptions.getType().toLowerCase(Locale.US);
    String sourceUri =
        String.format("gs://%s/%s/%s", TestConstants.TEMPORARY_GCS_BUCKET, table, dataFileName);
    uploadFileToGCS(dataFileName, sourceUri, mimeType);
    this.gcsObjectsToClean.add(sourceUri);
    IntegrationTestUtils.createBigLakeTable(
        testDataset.toString(),
        table,
        TestConstants.SHAKESPEARE_TABLE_SCHEMA,
        sourceUri,
        formatOptions);

    return testRunner.run(
        ReadIntegrationTestBase::readBigLakeTableApp,
        testDataset.toString(),
        table,
        ImmutableMap.of());
  }

  @Test
  public void testReadFromTableSnapshot() throws Exception {
    String snapshot =
        String.format("%s.%s.%s_snapshot", TestConstants.PROJECT_ID, testDataset, testTable);
    String allTypes =
        String.format(
            "%s.%s.%s", TestConstants.PROJECT_ID, testDataset, TestConstants.ALL_TYPES_TABLE_NAME);
    IntegrationTestUtils.runQuery(
        String.format("CREATE SNAPSHOT TABLE `%s` CLONE `%s`", snapshot, allTypes));

    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readTableSnapshotApp,
            testDataset.toString(),
            "",
            ImmutableMap.of("snapshot", snapshot, "allTypes", allTypes));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("matches").getAsBoolean()).isTrue();
  }

  @Test
  public void testReadFromTableWithSpacesInName() throws Exception {
    String tableNameWithSpaces = testTable + " with spaces";
    String tableIdForBqSql =
        String.format("`%s`.`%s`", testDataset.toString(), tableNameWithSpaces);
    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE %s (id INT64, data STRING) AS SELECT * FROM UNNEST([(1, 'foo'), (2, 'bar'), (3, 'baz')])",
            tableIdForBqSql));

    String tableIdForConnector =
        String.format("%s.%s", testDataset.toString(), tableNameWithSpaces);

    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readTableWithSpacesApp,
            testDataset.toString(),
            "",
            ImmutableMap.of("tableId", tableIdForConnector));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("count").getAsLong()).isEqualTo(3L);
    assertThat(result.get("filteredCount").getAsLong()).isEqualTo(2L);
    assertThat(result.get("sortedFirstId").getAsLong()).isEqualTo(2L);
    assertThat(result.get("sortedFirstData").getAsString()).isEqualTo("bar");
  }

  @Test
  public void testCreateReadSessionTimeout() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readSessionTimeoutApp,
            "",
            "",
            ImmutableMap.of(
                "scenario",
                "HIGH_TIMEOUT",
                "table",
                TestConstants.SHAKESPEARE_TABLE,
                "timeout",
                "1000"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
  }

  @Test
  @Ignore("Test has become flaky")
  public void testCreateReadSessionTimeoutWithLessTimeOnHugeData() {
    assertThrows(
        "DEADLINE_EXCEEDED: deadline exceeded ",
        DeadlineExceededException.class,
        () -> {
          testRunner.run(
              ReadIntegrationTestBase::readSessionTimeoutApp,
              "",
              "",
              ImmutableMap.of(
                  "scenario",
                  "LOW_TIMEOUT",
                  "table",
                  TestConstants.PUBLIC_DATA_WIKIPEDIA_PAGEVIEWS_2021,
                  "timeout",
                  "1"));
        });
  }

  @Test
  public void testNestedFieldProjection() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readNestedFieldProjectionApp, "", "", ImmutableMap.of());
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("rowCount").getAsInt()).isEqualTo(4);
  }

  @Test
  public void testReadFilteredTimestampField() throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone("PST"));

    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE `%s.%s` AS SELECT TIMESTAMP(\"2023-01-09 10:00:00\") as eventTime;",
            testDataset, testTable));

    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readFilteredTimestampApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of());
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("rowCount").getAsInt()).isEqualTo(1);
    assertThat(result.get("eventTimeMillis").getAsLong())
        .isEqualTo(Timestamp.valueOf("2023-01-09 02:00:00").getTime());
  }

  @Test
  public void testArrowTimestampRebaseOption() throws Exception {
    assumeTrue(isRebaseDateTimeAvailable());

    TimeZone originalTz = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      IntegrationTestUtils.runQuery(
          String.format(
              "CREATE TABLE `%s.%s` AS SELECT TIMESTAMP('1500-01-01 00:00:00+00') AS ts;",
              testDataset, testTable));

      JsonObject result =
          testRunner.run(
              ReadIntegrationTestBase::readArrowTimestampRebaseApp,
              testDataset.toString(),
              testTable,
              ImmutableMap.of());
      assertThat(result.get("status").getAsString()).isEqualTo("success");

      long withoutRebase = result.get("withoutRebaseMillis").getAsLong();
      long withRebase = result.get("withRebaseMillis").getAsLong();

      assertThat(withoutRebase).isEqualTo(Timestamp.valueOf("1500-01-01 00:00:00").getTime());
      assertThat(withRebase).isNotEqualTo(withoutRebase);
    } finally {
      TimeZone.setDefault(originalTz);
    }
  }

  private static boolean isRebaseDateTimeAvailable() {
    try {
      Class.forName("org.apache.spark.sql.catalyst.util.RebaseDateTime");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  @Test
  public void testPushDateTimePredicate() throws Exception {
    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE `%s.%s` (%s INTEGER, %s DATETIME) "
                + "AS SELECT * FROM UNNEST([(1, DATETIME '2023-09-25 1:00:00'), "
                + "(2, DATETIME '2023-09-29 10:00:00'), (3, DATETIME '2023-10-30 17:30:00')])",
            testDataset, testTable, "orderId", "orderDateTime"));

    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readPushDateTimePredicateApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of());
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("count").getAsLong()).isEqualTo(2L);
  }

  @Test
  public void testPseudoColumnsRuntimeFilteringDate() throws Exception {
    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE `%s.%s` (%s INT64) PARTITION BY _PARTITIONDATE ",
            testDataset, testTable, "orderId"));
    IntegrationTestUtils.runQuery(
        String.format(
            "INSERT INTO `%s.%s` (%s, _PARTITIONTIME) VALUES "
                + "(101, \"2024-01-05 00:00:00 UTC\"),"
                + "(102, \"2024-01-05 00:00:00 UTC\"),"
                + "(201, \"2024-01-10 00:00:00 UTC\"),"
                + "(202, \"2024-01-10 00:00:00 UTC\");",
            testDataset, testTable, "orderId"));
    String dt = "2024-01-05";

    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readPseudoColumnsRuntimeFilteringApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "DATE", "filter", "_PARTITIONDATE = '" + dt + "'"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("rowCount").getAsInt()).isEqualTo(2);
    assertThat(result.get("firstId").getAsLong()).isEqualTo(101L);
    assertThat(result.get("secondId").getAsLong()).isEqualTo(102L);
  }

  @Test
  public void testPseudoColumnsRuntimeFilteringHour() throws Exception {
    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE `%s.%s` (%s INT64) PARTITION BY TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR) ",
            testDataset, testTable, "orderId"));
    IntegrationTestUtils.runQuery(
        String.format(
            "INSERT INTO `%s.%s` (%s, _PARTITIONTIME) VALUES "
                + "(101, \"2024-01-05 18:00:00 UTC\"),"
                + "(102, \"2024-01-05 18:00:00 UTC\"),"
                + "(201, \"2024-01-10 06:00:00 UTC\"),"
                + "(202, \"2024-01-10 08:00:00 UTC\");",
            testDataset, testTable, "orderId"));
    String dt = "2024-01-05 18:00:00 UTC";

    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readPseudoColumnsRuntimeFilteringApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "HOUR", "filter", "_PARTITIONTIME = '" + dt + "'"));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("rowCount").getAsInt()).isEqualTo(2);
    assertThat(result.get("firstId").getAsLong()).isEqualTo(101L);
    assertThat(result.get("secondId").getAsLong()).isEqualTo(102L);
  }

  @Test
  public void testExecuteCommand() throws Exception {
    String sparkVersion = org.apache.spark.package$.MODULE$.SPARK_VERSION();
    assumeTrue(
        sparkVersion.startsWith("3.4")
            || sparkVersion.startsWith("3.5")
            || sparkVersion.startsWith("4."));
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readExecuteCommandApp,
            "",
            "",
            ImmutableMap.of(
                "query",
                String.format(
                    "CREATE TABLE %s.%s AS SELECT 1 AS `id`, 'foo' AS `name`",
                    testDataset.testDataset, testTable)));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("count").getAsLong()).isEqualTo(0L);

    Table table = bigQuery.getTable(testDataset.testDataset, testTable);
    assertThat(table).isNotNull();
    assertThat(table.getDefinition().getSchema().getFields()).hasSize(2);
  }

  @Test
  public void testReadDataTypes() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readDataTypesApp,
            testDataset.toString(),
            "",
            ImmutableMap.of("timestamp_ntz_type", String.valueOf(timeStampNTZType.isPresent())));
    assertThat(result.get("status").getAsString()).isEqualTo("success");
  }

  @Test
  public void testCustomAccessTokenProvider() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadIntegrationTestBase::readCustomAccessTokenProviderApp, "", "", ImmutableMap.of());
    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("count").getAsLong()).isEqualTo(TestConstants.SHAKESPEARE_TABLE_NUM_ROWS);
  }
}
