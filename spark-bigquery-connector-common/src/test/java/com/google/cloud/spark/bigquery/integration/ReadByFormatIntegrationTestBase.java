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

import static com.google.cloud.spark.bigquery.integration.TestConstants.GA4_TABLE;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
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
import com.google.cloud.spark.bigquery.integration.model.ColumnOrderTestClass;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.gson.JsonObject;
import java.time.LocalDateTime;
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
          spark.read().format("bigquery").load(String.format("%s.%s", testDataset, testTable));
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
            ImmutableMap.of());

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
      LocalDateTime dateTime = LocalDateTime.of(2023, 9, 18, 14, 30, 15, 234 * 1_000_000);
      bigQuery.create(
          TableInfo.newBuilder(
                  TableId.of(testDataset, testTable),
                  StandardTableDefinition.of(
                      Schema.of(Field.of("foo", LegacySQLTypeName.DATETIME))))
              .build());

      IntegrationTestUtils.runQuery(
          String.format("INSERT INTO %s.%s (foo) VALUES ('%s')", testDataset, testTable, dateTime));

      Dataset<Row> df =
          spark
              .read()
              .format("bigquery")
              .option("dataset", testDataset)
              .option("table", testTable)
              .load();

      StructType schema = df.schema();
      boolean typeCorrect = schema.apply("foo").dataType().typeName().equals(expectedTypeName);

      Row row = df.head();
      LocalDateTime val = (LocalDateTime) row.get(0);
      boolean dateTimeMatches = dateTime.equals(val);

      JsonObject result = new JsonObject();
      result.addProperty("status", "success");
      result.addProperty("typeCorrect", typeCorrect);
      result.addProperty("dateTimeMatches", dateTimeMatches);
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
    assumeTrue("This test only works for AVRO dataformat", dataFormat.equals("AVRO"));
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

  protected Set<String> extractWords(Dataset<Row> df) {
    return ImmutableSet.copyOf(
        df.select("word").where("corpus_date = 0").as(Encoders.STRING()).collectAsList());
  }
}
