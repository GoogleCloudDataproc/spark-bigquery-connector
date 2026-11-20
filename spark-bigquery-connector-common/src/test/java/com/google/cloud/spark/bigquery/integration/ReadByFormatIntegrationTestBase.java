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

import static com.google.cloud.spark.bigquery.integration.DataTypeTestValues.SCALAR_TYPES;
import static com.google.cloud.spark.bigquery.integration.TestConstants.GA4_TABLE;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation;
import com.google.cloud.spark.bigquery.integration.DataTypeTestValues.ScalarType;
import com.google.cloud.spark.bigquery.integration.model.ColumnOrderTestClass;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class ReadByFormatIntegrationTestBase extends SparkBigQueryIntegrationTestBaseV2 {

  protected SparkBigQueryIntegrationTestRunner testRunner =
      new InMemorySparkBigQueryIntegrationTestRunner();

  private SparkSession sparkSession;

  protected SparkSession spark() {
    if (sparkSession == null) {
      sparkSession =
          IntegrationTestUtils.createSparkSessionBuilder("ReadByFormatTest")
              .getOrCreate()
              .newSession();
    }
    return sparkSession;
  }

  private static final int LARGE_TABLE_NUMBER_OF_PARTITIONS = 138;
  protected final String dataFormat;
  protected final boolean userProvidedSchemaAllowed;
  protected Optional<DataType> timeStampNTZType;
  protected DataSourceVersion dataSourceVersion = DataSourceVersion.V2;

  public ReadByFormatIntegrationTestBase(String dataFormat) {
    this(dataFormat, true, Optional.empty());
  }

  public ReadByFormatIntegrationTestBase(String dataFormat, boolean userProvidedSchemaAllowed) {
    this(dataFormat, userProvidedSchemaAllowed, Optional.empty());
  }

  public ReadByFormatIntegrationTestBase(
      String dataFormat, boolean userProvidedSchemaAllowed, DataType timestampNTZType) {
    this(dataFormat, userProvidedSchemaAllowed, Optional.of(timestampNTZType));
  }

  public ReadByFormatIntegrationTestBase(
      String dataFormat, boolean userProvidedSchemaAllowed, Optional<DataType> timestampNTZType) {
    super();
    this.dataFormat = dataFormat;
    this.userProvidedSchemaAllowed = userProvidedSchemaAllowed;
    this.timeStampNTZType = timestampNTZType;
  }

  // =========================================================================
  // SCENARIO: Read View select-and-filter pushdowns
  // =========================================================================

  @SuppressWarnings("resource")
  protected static JsonObject readByFormatViewApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String scenario = parameters.getOrDefault("scenario", "VIEW");
    String format = parameters.get("dataFormat");

    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadByFormatViewTestApp")
            .getOrCreate()
            .newSession();

    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset)
            .option("table", TestConstants.SHAKESPEARE_VIEW)
            .option("viewsEnabled", "true")
            .option("viewMaterializationProject", System.getenv("GOOGLE_CLOUD_PROJECT"))
            .option("viewMaterializationDataset", testDataset)
            .option("readDataFormat", format)
            .load();

    if ("CACHED_VIEW".equals(scenario)) {
      df = df.cache();
    }

    List<Row> resultList = df.select("corpus").filter("word = 'spark'").collectAsList();
    long filteredCount =
        resultList.stream().filter(row -> "hamlet".equals(row.getString(0))).count();

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    result.addProperty("rowCount", resultList.size());
    result.addProperty("filteredCount", filteredCount);
    return result;
  }

  @Test
  public void testViewWithDifferentColumnsForSelectAndFilter() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readByFormatViewApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "VIEW", "dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("rowCount").getAsInt()).isEqualTo(9);
    assertThat(result.get("filteredCount").getAsInt()).isEqualTo(1);
  }

  @Test
  public void testCachedViewWithDifferentColumnsForSelectAndFilter() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readByFormatViewApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "CACHED_VIEW", "dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("rowCount").getAsInt()).isEqualTo(9);
    assertThat(result.get("filteredCount").getAsInt()).isEqualTo(1);
  }

  // =========================================================================
  // SCENARIO: Read Out-Of-Order / Selected Columns
  // =========================================================================

  @SuppressWarnings("resource")
  protected static JsonObject readByFormatColumnsApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String scenario = parameters.getOrDefault("scenario", "OUT_OF_ORDER");
    String format = parameters.get("dataFormat");

    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadByFormatColumnsTestApp")
            .getOrCreate()
            .newSession();

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");

    if ("OUT_OF_ORDER".equals(scenario)) {
      Row row =
          spark
              .read()
              .format("bigquery")
              .option("table", TestConstants.SHAKESPEARE_TABLE)
              .option("readDataFormat", format)
              .load()
              .select("word_count", "word")
              .head();

      result.addProperty("isCol0Long", row.get(0) instanceof Long);
      result.addProperty("isCol1String", row.get(1) instanceof String);

    } else if ("ALL_COLUMNS".equals(scenario)) {
      Row row =
          spark
              .read()
              .format("bigquery")
              .option("table", TestConstants.SHAKESPEARE_TABLE)
              .option("readDataFormat", format)
              .load()
              .select("word_count", "word", "corpus", "corpus_date")
              .head();

      result.addProperty("isCol0Long", row.get(0) instanceof Long);
      result.addProperty("isCol1String", row.get(1) instanceof String);
      result.addProperty("isCol2String", row.get(2) instanceof String);
      result.addProperty("isCol3Long", row.get(3) instanceof Long);
    }

    return result;
  }

  @Test
  public void testOutOfOrderColumns() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readByFormatColumnsApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "OUT_OF_ORDER", "dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("isCol0Long").getAsBoolean()).isTrue();
    assertThat(result.get("isCol1String").getAsBoolean()).isTrue();
  }

  @Test
  public void testSelectAllColumnsFromATable() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readByFormatColumnsApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "ALL_COLUMNS", "dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("isCol0Long").getAsBoolean()).isTrue();
    assertThat(result.get("isCol1String").getAsBoolean()).isTrue();
    assertThat(result.get("isCol2String").getAsBoolean()).isTrue();
    assertThat(result.get("isCol3Long").getAsBoolean()).isTrue();
  }

  // =========================================================================
  // SCENARIO: Read Parallelism and Partitions Split balanced checks
  // =========================================================================

  @SuppressWarnings("resource")
  protected static JsonObject readByFormatPartitionsApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String scenario = parameters.getOrDefault("scenario", "CUSTOM_PARTITIONS");
    String format = parameters.get("dataFormat");

    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadByFormatPartitionsTestApp")
            .getOrCreate()
            .newSession();

    try {
      JsonObject result = new JsonObject();
      result.addProperty("status", "success");

      if ("CUSTOM_PARTITIONS".equals(scenario)) {
        Dataset<Row> df =
            spark
                .read()
                .format("bigquery")
                .option("table", TestConstants.LARGE_TABLE)
                .option("maxParallelism", "5")
                .option("preferredMinParallelism", "5")
                .option("readDataFormat", format)
                .load();
        result.addProperty("numPartitions", df.rdd().getNumPartitions());

      } else if ("DEFAULT_PARTITIONS".equals(scenario)) {
        Dataset<Row> df =
            spark
                .read()
                .format("bigquery")
                .option("table", TestConstants.LARGE_TABLE)
                .option("readDataFormat", format)
                .load();
        result.addProperty("numPartitions", df.rdd().getNumPartitions());

      } else if ("BALANCED_PARTITIONS".equals(scenario)) {
        Dataset<Row> df =
            spark
                .read()
                .format("bigquery")
                .option("maxParallelism", "5")
                .option("preferredMinParallelism", "5")
                .option("readDataFormat", format)
                .option("filter", "year > 2000")
                .load(TestConstants.LARGE_TABLE)
                .select(TestConstants.LARGE_TABLE_FIELD);

        long sizeOfFirstPartition =
            df.rdd()
                .toJavaRDD()
                .mapPartitions(rows -> Arrays.asList(Iterators.size(rows)).iterator())
                .collect()
                .get(0)
                .longValue();

        result.addProperty("numPartitions", df.rdd().getNumPartitions());
        result.addProperty("sizeOfFirstPartition", sizeOfFirstPartition);
      }
      return result;
    } finally {
    }
  }

  @Test
  public void testNumberOfPartitions() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readByFormatPartitionsApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "CUSTOM_PARTITIONS", "dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("numPartitions").getAsInt()).isEqualTo(5);
  }

  @Test
  public void testDefaultNumberOfPartitions() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readByFormatPartitionsApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "DEFAULT_PARTITIONS", "dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("numPartitions").getAsInt()).isEqualTo(LARGE_TABLE_NUMBER_OF_PARTITIONS);
  }

  @Test(timeout = 300_000)
  public void testBalancedPartitions() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readByFormatPartitionsApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "BALANCED_PARTITIONS", "dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    int numPartitions = result.get("numPartitions").getAsInt();
    long sizeOfFirstPartition = result.get("sizeOfFirstPartition").getAsLong();

    long idealPartitionSize = TestConstants.LARGE_TABLE_NUM_ROWS / numPartitions;
    assertThat(sizeOfFirstPartition).isAtLeast((int) (idealPartitionSize * 0.9));
    assertThat(sizeOfFirstPartition).isAtMost((int) (idealPartitionSize * 1.1));
  }

  // =========================================================================
  // SCENARIO: Read combinePushedDownFilters behaviour checks
  // =========================================================================

  @SuppressWarnings("resource")
  protected static JsonObject readKeepingFiltersApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String format = parameters.get("dataFormat");

    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadKeepingFiltersTestApp")
            .getOrCreate()
            .newSession();

    try {
      Dataset<Row> df1 =
          spark
              .read()
              .format("bigquery")
              .option("table", "bigquery-public-data.samples.shakespeare")
              .option("filter", "length(word) = 1")
              .option("combinePushedDownFilters", "true")
              .option("readDataFormat", format)
              .load();

      Dataset<Row> df2 =
          spark
              .read()
              .format("bigquery")
              .option("table", "bigquery-public-data.samples.shakespeare")
              .option("filter", "length(word) = 1")
              .option("combinePushedDownFilters", "false")
              .option("readDataFormat", format)
              .load();

      List<String> newWords =
          df1.select("word").where("corpus_date = 0").as(Encoders.STRING()).collectAsList();
      List<String> oldWords =
          df2.select("word").where("corpus_date = 0").as(Encoders.STRING()).collectAsList();

      boolean equal = new java.util.HashSet<>(newWords).equals(new java.util.HashSet<>(oldWords));

      JsonObject result = new JsonObject();
      result.addProperty("status", "success");
      result.addProperty("equal", equal);
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Error in readKeepingFiltersApp", e);
    }
  }

  @Test
  public void testKeepingFiltersBehaviour() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readKeepingFiltersApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("equal").getAsBoolean()).isTrue();
  }

  // =========================================================================
  // SCENARIO: Read Column Order Struct Schema verification
  // =========================================================================

  @SuppressWarnings("resource")
  protected static JsonObject readColumnOrderStructApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String format = parameters.get("dataFormat");

    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadColumnOrderStructTestApp")
            .getOrCreate()
            .newSession();

    try {
      StructType schema = Encoders.bean(ColumnOrderTestClass.class).schema();

      Dataset<ColumnOrderTestClass> dataset =
          spark
              .read()
              .schema(schema)
              .option("dataset", testDataset)
              .option("table", TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_NAME)
              .format("bigquery")
              .option("readDataFormat", format)
              .load()
              .as(Encoders.bean(ColumnOrderTestClass.class));

      ColumnOrderTestClass row = dataset.head();
      boolean equal = row.equals(TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_COLS);

      JsonObject result = new JsonObject();
      result.addProperty("status", "success");
      result.addProperty("equal", equal);
      return result;
    } finally {
    }
  }

  @Test
  public void testColumnOrderOfStruct() throws Exception {
    assumeTrue("user provided schema is not allowed for this connector", userProvidedSchemaAllowed);
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readColumnOrderStructApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("equal").getAsBoolean()).isTrue();
  }

  // =========================================================================
  // SCENARIO: Read BQ Map and convert to Spark Map
  // =========================================================================

  @SuppressWarnings("resource")
  protected static JsonObject readConvertMapApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadConvertMapTestApp")
            .getOrCreate()
            .newSession();

    try {
      BigQuery bigQuery = IntegrationTestUtils.getBigquery();
      bigQuery.create(
          TableInfo.newBuilder(
                  TableId.of(testDataset, testTable),
                  StandardTableDefinition.of(
                      Schema.of(
                          Field.newBuilder(
                                  "map_field",
                                  LegacySQLTypeName.RECORD,
                                  FieldList.of(
                                      Field.newBuilder("key", LegacySQLTypeName.STRING)
                                          .setMode(Field.Mode.REQUIRED)
                                          .build(),
                                      Field.of("value", LegacySQLTypeName.INTEGER)))
                              .setMode(Field.Mode.REPEATED)
                              .build())))
              .build());

      IntegrationTestUtils.runQuery(
          String.format(
              "INSERT INTO %s.%s VALUES ([STRUCT('a' as key, 1 as value),STRUCT('b' as key, 2 as value)]),([STRUCT('c' as key, 3 as value)])",
              testDataset, testTable));

      Dataset<Row> df =
          spark
              .read()
              .format("bigquery")
              .option("readDataFormat", parameters.get("dataFormat"))
              .load(String.format("%s.%s", testDataset, testTable));
      StructType schema = df.schema();
      boolean schemaSizeCorrect = (schema.size() == 1);
      StructField mapField = schema.apply("map_field");
      boolean typeCorrect =
          mapField
              .dataType()
              .equals(DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType));

      List<Row> rowList = df.collectAsList();
      List<Map<?, ?>> list =
          rowList.stream().map(row -> row.getJavaMap(0)).collect(Collectors.toList());

      boolean containsMap1 = list.contains(ImmutableMap.of("a", 1L, "b", 2L));
      boolean containsMap2 = list.contains(ImmutableMap.of("c", 3L));

      JsonObject result = new JsonObject();
      result.addProperty("status", "success");
      result.addProperty("schemaSizeCorrect", schemaSizeCorrect);
      result.addProperty("typeCorrect", typeCorrect);
      result.addProperty("rowCount", rowList.size());
      result.addProperty("containsMap1", containsMap1);
      result.addProperty("containsMap2", containsMap2);
      return result;
    } finally {
    }
  }

  @Test
  public void testConvertBigQueryMapToSparkMap() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readConvertMapApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("schemaSizeCorrect").getAsBoolean()).isTrue();
    assertThat(result.get("typeCorrect").getAsBoolean()).isTrue();
    assertThat(result.get("rowCount").getAsInt()).isEqualTo(2);
    assertThat(result.get("containsMap1").getAsBoolean()).isTrue();
    assertThat(result.get("containsMap2").getAsBoolean()).isTrue();
  }

  // =========================================================================
  // SCENARIO: Read Timestamp NTZ Datetime fields
  // =========================================================================

  @SuppressWarnings("resource")
  protected static JsonObject readTimestampNTZApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String expectedTypeName = parameters.get("ntzTypeName");

    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadTimestampNTZTestApp")
            .getOrCreate()
            .newSession();

    try {
      BigQuery bigQuery = IntegrationTestUtils.getBigquery();
      List<LocalDateTime> expectedDateTimes =
          Arrays.asList(
              LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_456_000),
              LocalDateTime.of(1970, 1, 1, 0, 0, 0),
              LocalDateTime.of(2023, 9, 18, 14, 30, 15, 234 * 1_000_000),
              // Largest fraction DATETIME supports (microsecond precision)
              LocalDateTime.of(2023, 9, 18, 14, 30, 15, 999_999_000),
              LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_456_000),
              LocalDateTime.of(2026, 3, 19, 5, 0, 0));
      bigQuery.create(
          TableInfo.newBuilder(
                  TableId.of(testDataset, testTable),
                  StandardTableDefinition.of(
                      Schema.of(Field.of("foo", LegacySQLTypeName.DATETIME))))
              .build());

      String valuesSql =
          expectedDateTimes.stream()
              .map(dt -> String.format("('%s')", dt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
              .collect(Collectors.joining(", "));
      IntegrationTestUtils.runQuery(
          String.format("INSERT INTO %s.%s (foo) VALUES %s", testDataset, testTable, valuesSql));

      Dataset<Row> avroDf =
          spark
              .read()
              .format("bigquery")
              .option("dataset", testDataset)
              .option("table", testTable)
              .option("readDataFormat", "AVRO")
              .load()
              .orderBy("foo");

      Dataset<Row> arrowDf =
          spark
              .read()
              .format("bigquery")
              .option("dataset", testDataset)
              .option("table", testTable)
              .option("readDataFormat", "ARROW")
              .load()
              .orderBy("foo");

      StructType avroSchema = avroDf.schema();
      StructType arrowSchema = arrowDf.schema();
      boolean typeCorrect =
          avroSchema.apply("foo").dataType().typeName().equals(expectedTypeName)
              && avroSchema.equals(arrowSchema);

      List<Row> avroRows = avroDf.collectAsList();
      List<Row> arrowRows = arrowDf.collectAsList();
      boolean dataframesEqual = avroRows.equals(arrowRows);

      List<LocalDateTime> actualDateTimes =
          avroRows.stream().map(row -> (LocalDateTime) row.get(0)).collect(Collectors.toList());
      boolean dateTimeMatches = expectedDateTimes.equals(actualDateTimes);

      JsonObject result = new JsonObject();
      result.addProperty("status", "success");
      result.addProperty("typeCorrect", typeCorrect);
      result.addProperty("dateTimeMatches", dateTimeMatches);
      result.addProperty("dataframesEqual", dataframesEqual);
      return result;
    } finally {
    }
  }

  @Test
  public void testTimestampNTZReadFromBigQuery() throws Exception {
    assumeThat(timeStampNTZType.isPresent(), is(true));
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readTimestampNTZApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("ntzTypeName", timeStampNTZType.get().typeName()));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("typeCorrect").getAsBoolean()).isTrue();
    assertThat(result.get("dateTimeMatches").getAsBoolean()).isTrue();
    assertThat(result.get("dataframesEqual").getAsBoolean()).isTrue();
  }

  // =========================================================================
  // SCENARIO: Read Window functions calculations (GA4 tables)
  // =========================================================================

  @SuppressWarnings("resource")
  protected static JsonObject readGA4WindowFunctionApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String scenario = parameters.getOrDefault("scenario", "STANDARD");
    String format = parameters.get("dataFormat");

    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadGA4WindowFunctionTestApp")
            .getOrCreate()
            .newSession();

    try {
      JsonObject result = new JsonObject();
      result.addProperty("status", "success");

      if ("STANDARD".equals(scenario)) {
        WindowSpec windowSpec =
            Window.partitionBy("user_pseudo_id", "event_timestamp", "event_name")
                .orderBy("event_bundle_sequence_id");

        Dataset<Row> df =
            spark
                .read()
                .format("bigquery")
                .option("table", GA4_TABLE)
                .option("readDataFormat", format)
                .load()
                .withColumn("row_num", row_number().over(windowSpec));

        Dataset<Row> selectedDF =
            df.select("user_pseudo_id", "event_name", "event_timestamp", "row_num");
        long rowNumCount =
            Arrays.stream(df.schema().fields())
                .filter(field -> field.name().equals("row_num"))
                .count();

        result.addProperty("columnsLength", selectedDF.columns().length);
        result.addProperty("rowNumFieldExists", rowNumCount == 1);
        Row headRow = selectedDF.head();
        result.addProperty("headRowNumValue", headRow.getInt(headRow.fieldIndex("row_num")));

      } else if ("WITH_ARRAY".equals(scenario)) {
        WindowSpec windowSpec =
            Window.partitionBy(
                    concat(col("user_pseudo_id"), col("event_timestamp"), col("event_name")))
                .orderBy(lit("window_ordering"));

        Dataset<Row> df =
            spark
                .read()
                .format("bigquery")
                .option("table", GA4_TABLE)
                .option("readDataFormat", format)
                .load()
                .withColumn("row_num", row_number().over(windowSpec));

        Dataset<Row> selectedDF =
            df.select("user_pseudo_id", "event_name", "event_timestamp", "event_params", "row_num");
        long rowNumCount =
            Arrays.stream(df.schema().fields())
                .filter(field -> field.name().equals("row_num"))
                .count();

        Row head = selectedDF.head();
        result.addProperty("columnsLength", selectedDF.columns().length);
        result.addProperty("rowNumFieldExists", rowNumCount == 1);
        result.addProperty("headRowNumValue", head.getInt(head.fieldIndex("row_num")));
      }

      return result;
    } finally {
    }
  }

  @Test
  public void testWindowFunctionPartitionBy() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readGA4WindowFunctionApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "STANDARD", "dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("columnsLength").getAsInt()).isEqualTo(4);
    assertThat(result.get("rowNumFieldExists").getAsBoolean()).isTrue();
    assertThat(result.get("headRowNumValue").getAsInt()).isEqualTo(1);
  }

  @Test
  public void testWindowFunctionPartitionByWithArray() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readGA4WindowFunctionApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "WITH_ARRAY", "dataFormat", dataFormat));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("columnsLength").getAsInt()).isEqualTo(5);
    assertThat(result.get("rowNumFieldExists").getAsBoolean()).isTrue();
    assertThat(result.get("headRowNumValue").getAsInt()).isEqualTo(1);
  }

  Dataset<Row> getViewDataFrame() {
    return spark()
        .read()
        .format("bigquery")
        .option("dataset", testDataset.toString())
        .option("table", TestConstants.SHAKESPEARE_VIEW)
        .option("viewsEnabled", "true")
        .option("viewMaterializationProject", System.getenv("GOOGLE_CLOUD_PROJECT"))
        .option("viewMaterializationDataset", testDataset.toString())
        .option("readDataFormat", dataFormat)
        .load();
  }

  Dataset<Row> readAllTypesTable() {
    return spark()
        .read()
        .format("bigquery")
        .option("dataset", testDataset.toString())
        .option("table", TestConstants.ALL_TYPES_TABLE_NAME)
        .load();
  }

  protected static JsonObject optimizedCountStarApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String dataFormat = parameters.get("dataFormat");
    boolean withFilter = Boolean.parseBoolean(parameters.getOrDefault("withFilter", "false"));

    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("OptimizedCountStarTestApp")
            .getOrCreate()
            .newSession();

    DirectBigQueryRelation.emptyRowRDDsCreated = 0;

    Dataset<Row> df1 =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("optimizedEmptyProjection", "false")
            .option("readDataFormat", dataFormat)
            .load();

    if (withFilter) {
      df1 = df1.select("corpus_date").where("corpus_date > 0");
    } else {
      df1 = df1.select("corpus_date");
    }
    long oldMethodCount = df1.count();
    int emptyRowRDDsCreated1 = DirectBigQueryRelation.emptyRowRDDsCreated;

    Dataset<Row> df2 =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", dataFormat)
            .load();

    if (withFilter) {
      df2 = df2.where("corpus_date > 0");
    }
    long optimizedCount = df2.count();
    int emptyRowRDDsCreated2 = DirectBigQueryRelation.emptyRowRDDsCreated;

    JsonObject result = new JsonObject();
    result.addProperty("oldMethodCount", oldMethodCount);
    result.addProperty("optimizedCount", optimizedCount);
    result.addProperty("emptyRowRDDsCreated1", emptyRowRDDsCreated1);
    result.addProperty("emptyRowRDDsCreated2", emptyRowRDDsCreated2);
    return result;
  }

  @Test
  public void testOptimizedCountStarWithFilter() throws Exception {
    assumeThat(dataSourceVersion, equalTo(DataSourceVersion.V1));
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::optimizedCountStarApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("withFilter", "true", "dataFormat", dataFormat));

    assertThat(result.get("emptyRowRDDsCreated1").getAsInt()).isEqualTo(0);
    assertThat(result.get("optimizedCount").getAsLong())
        .isEqualTo(result.get("oldMethodCount").getAsLong());
    assertThat(result.get("emptyRowRDDsCreated2").getAsInt()).isEqualTo(1);
  }

  @Test
  public void testOptimizedCountStar() throws Exception {
    assumeThat(dataSourceVersion, equalTo(DataSourceVersion.V1));
    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::optimizedCountStarApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("withFilter", "false", "dataFormat", dataFormat));

    assertThat(result.get("emptyRowRDDsCreated1").getAsInt()).isEqualTo(0);
    assertThat(result.get("optimizedCount").getAsLong())
        .isEqualTo(result.get("oldMethodCount").getAsLong());
    assertThat(result.get("emptyRowRDDsCreated2").getAsInt()).isEqualTo(1);
  }

  // =========================================================================
  // SCENARIO: Read every scalar type, nested in structs, arrays and maps
  // =========================================================================

  // More rows than an Arrow record batch holds (4096), so the read spans several batches.
  private static final int DATA_TYPES_NUM_ROWS = 5000;

  @SuppressWarnings("resource")
  protected static JsonObject readDataTypesApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {
    @SuppressWarnings("resource")
    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadDataTypesTestApp")
            .getOrCreate()
            .newSession();
    Dataset<Row> actual =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset)
            .option("table", testTable)
            .option("readDataFormat", parameters.get("dataFormat"))
            .load();
    Dataset<Row> expected =
        spark.range(DATA_TYPES_NUM_ROWS).selectExpr(parameters.get("expectedColumns").split("\n"));

    JsonObject result = new JsonObject();
    result.addProperty("status", "success");
    result.addProperty("actualSchema", actual.schema().simpleString());
    result.addProperty("expectedSchema", expected.schema().simpleString());
    // to_json gives a comparable form for binary, map and nested values. The timestamp formats keep
    // the microseconds, which the default formats drop.
    List<String> actualRows = toJsonRows(actual);
    List<String> expectedRows = toJsonRows(expected);
    result.addProperty("numRows", actualRows.size());
    JsonArray mismatches = new JsonArray();
    if (actualRows.size() != expectedRows.size()) {
      mismatches.add(
          String.format(
              "row count: actual %d, expected %d", actualRows.size(), expectedRows.size()));
    }
    for (int i = 0; i < Math.min(actualRows.size(), expectedRows.size()); i++) {
      if (!actualRows.get(i).equals(expectedRows.get(i)) && mismatches.size() < 5) {
        mismatches.add("actual:   " + actualRows.get(i) + "\nexpected: " + expectedRows.get(i));
      }
    }
    result.add("mismatches", mismatches);
    return result;
  }

  private static List<String> toJsonRows(Dataset<Row> df) {
    Map<String, String> options =
        ImmutableMap.of(
            "timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
            "timestampNTZFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    return df.orderBy("id")
        .select(to_json(struct(col("*")), options))
        .as(Encoders.STRING())
        .collectAsList();
  }

  @Test
  public void testReadDataTypes() throws Exception {
    // DATETIME columns use TIMESTAMP_NTZ, which Spark supports from 3.4.
    List<ScalarType> types =
        SCALAR_TYPES.stream()
            .filter(type -> timeStampNTZType.isPresent() || !type.name.equals("datetime"))
            .collect(Collectors.toList());
    List<String> columns = new ArrayList<>();
    List<String> bigQueryValues = new ArrayList<>();
    List<String> expectedColumns = new ArrayList<>();
    columns.add("id INT64");
    bigQueryValues.add("i");
    expectedColumns.add("id");
    // Every third row has NULL scalars, a NULL struct (on a different row) and empty arrays and
    // maps, since BigQuery stores a NULL array as an empty one.
    for (ScalarType type : types) {
      columns.add(String.format("scalar_%s %s", type.name, type.bigQueryType));
      bigQueryValues.add(String.format("IF(MOD(i, 3) = 0, NULL, %s)", type.bigQueryValue("i")));
      expectedColumns.add(
          String.format(
              "IF(id %% 3 = 0, NULL, %s) AS scalar_%s", type.sparkValue("id"), type.name));
    }
    columns.add(
        types.stream()
            .map(type -> String.format("f_%s %s", type.name, type.bigQueryType))
            .collect(Collectors.joining(", ", "struct_all STRUCT<", ">")));
    bigQueryValues.add(
        types.stream()
            .map(type -> String.format("%s AS f_%s", type.bigQueryValue("i"), type.name))
            .collect(Collectors.joining(", ", "IF(MOD(i, 3) = 1, NULL, STRUCT(", "))")));
    expectedColumns.add(
        types.stream()
            .map(type -> String.format("'f_%s', %s", type.name, type.sparkValue("id")))
            .collect(
                Collectors.joining(
                    ", ", "IF(id % 3 = 1, NULL, named_struct(", ")) AS struct_all")));
    // ARRAY<INT64> and maps with INT64 values are read by other tests.
    for (ScalarType type : types) {
      if (type.name.equals("int64")) {
        continue;
      }
      // The Avro read path returns NULL elements for ARRAY<BIGNUMERIC>.
      boolean skipArray = dataFormat.equals("AVRO") && type.name.equals("bignumeric");
      if (!skipArray) {
        columns.add(String.format("array_%s ARRAY<%s>", type.name, type.bigQueryType));
        bigQueryValues.add(
            String.format(
                "IF(MOD(i, 3) = 0, [], [%s, %s])",
                type.bigQueryValue("i"), type.bigQueryValue("i + 1")));
        expectedColumns.add(
            String.format(
                "filter(array(%s, %s), e -> id %% 3 != 0) AS array_%s",
                type.sparkValue("id"), type.sparkValue("id + 1"), type.name));
      }
      columns.add(
          String.format(
              "map_%s ARRAY<STRUCT<key STRING NOT NULL, value %s>>", type.name, type.bigQueryType));
      bigQueryValues.add(
          String.format(
              "IF(MOD(i, 3) = 0, [], [STRUCT('a' AS key, %s AS value), STRUCT('b', %s)])",
              type.bigQueryValue("i"), type.bigQueryValue("i + 1")));
      expectedColumns.add(
          String.format(
              "map_filter(map('a', %s, 'b', %s), (k, v) -> id %% 3 != 0) AS map_%s",
              type.sparkValue("id"), type.sparkValue("id + 1"), type.name));
    }
    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE %s.%s (%s) AS SELECT %s FROM UNNEST(GENERATE_ARRAY(0, %d)) AS i",
            testDataset,
            testTable,
            String.join(", ", columns),
            String.join(", ", bigQueryValues),
            DATA_TYPES_NUM_ROWS - 1));

    JsonObject result =
        testRunner.run(
            ReadByFormatIntegrationTestBase::readDataTypesApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of(
                "dataFormat", dataFormat, "expectedColumns", String.join("\n", expectedColumns)));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("actualSchema").getAsString())
        .isEqualTo(result.get("expectedSchema").getAsString());
    assertThat(result.get("numRows").getAsInt()).isEqualTo(DATA_TYPES_NUM_ROWS);
    assertThat(result.getAsJsonArray("mismatches")).isEmpty();
  }

  protected Set<String> extractWords(Dataset<Row> df) {
    return ImmutableSet.copyOf(
        df.select("word").where("corpus_date = 0").as(Encoders.STRING()).collectAsList());
  }
}
