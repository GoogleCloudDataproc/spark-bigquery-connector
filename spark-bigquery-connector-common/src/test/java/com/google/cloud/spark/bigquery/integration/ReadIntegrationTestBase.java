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

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
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

public class ReadIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

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

  protected List<String> gcsObjectsToClean = new ArrayList<>();

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

  /**
   * Generate a test to verify that the given DataFrame is equal to a known result and contains
   * Nullable Schema.
   */
  private void testShakespeare(Dataset<Row> df) {
    assertThat(df.schema()).isEqualTo(SHAKESPEARE_TABLE_SCHEMA_WITH_METADATA_COMMENT);
    assertThat(df.count()).isEqualTo(TestConstants.SHAKESPEARE_TABLE_NUM_ROWS);
    List<String> firstWords =
        Arrays.asList(
            (String[])
                df.select("word")
                    .where("word >= 'a' AND word not like '%\\'%'")
                    .distinct()
                    .as(Encoders.STRING())
                    .sort("word")
                    .take(3));
    assertThat(firstWords).containsExactly("a", "abaissiez", "abandon");
  }

  @Test
  public void testReadWithOption() {
    testShakespeare(
        spark.read().format("bigquery").option("table", TestConstants.SHAKESPEARE_TABLE).load());
  }

  @Test
  public void testReadWithSimplifiedApi() {
    testShakespeare(spark.read().format("bigquery").load(TestConstants.SHAKESPEARE_TABLE));
  }

  @Test
  @Ignore("DSv2 only")
  public void testReadCompressed() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", TestConstants.SHAKESPEARE_TABLE)
            .option("bqEncodedCreateReadSessionRequest", "EgZCBBoCEAI=")
            .load();
    // Test early termination succeeds
    df.head();
    testShakespeare(df);
  }

  @Test
  @Ignore("DSv2 only")
  public void testReadCompressedWith1BackgroundThreads() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", TestConstants.SHAKESPEARE_TABLE)
            .option("bqEncodedCreateReadSessionRequest", "EgZCBBoCEAI=")
            .option("bqBackgroundThreadsPerStream", "1")
            .load();
    // Test early termination succeeds
    df.head();
    testShakespeare(df);
  }

  @Test
  @Ignore("DSv2 only")
  public void testReadCompressedWith4BackgroundThreads() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", TestConstants.SHAKESPEARE_TABLE)
            .option("bqEncodedCreateReadSessionRequest", "EgZCBBoCEAI=")
            .option("bqBackgroundThreadsPerStream", "4")
            .load();
    // Test early termination succeeds
    df.head();
    testShakespeare(df);
  }

  @Test
  public void testReadSchemaPruned() {
    Row res =
        readAllTypesTable()
            .select("str", "nested_struct.str", "nested_struct.inner_struct.str")
            .collectAsList()
            .get(0);
    assertThat(res.get(0)).isEqualTo("string");
    assertThat(res.get(1)).isEqualTo("stringa");
    assertThat(res.get(2)).isEqualTo("stringaa");
  }

  @Test
  public void testFilters() {
    Dataset<Row> df = spark.read().format("bigquery").load(TestConstants.SHAKESPEARE_TABLE);
    assertThat(df.schema()).isEqualTo(SHAKESPEARE_TABLE_SCHEMA_WITH_METADATA_COMMENT);
    assertThat(df.count()).isEqualTo(TestConstants.SHAKESPEARE_TABLE_NUM_ROWS);
    FILTER_DATA.forEach(
        (condition, expectedElements) -> {
          List<String> firstWords =
              Arrays.asList(
                  (String[])
                      df.select("word")
                          .where(condition)
                          .distinct()
                          .as(Encoders.STRING())
                          .sort("word")
                          .take(3));
          assertThat(firstWords).containsExactlyElementsIn(expectedElements);
        });
  }

  Dataset<Row> readAllTypesTable() {
    return spark
        .read()
        .format("bigquery")
        .option("dataset", testDataset.toString())
        .option("table", ALL_TYPES_TABLE_NAME)
        .load();
  }

  @Test
  public void testCountWithFilters() {
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

    assertThat(countResults).isEqualTo(countAfterCollect);
  }

  @Test
  public void testKnownSizeInBytes() {
    Dataset<Row> allTypesTable = readAllTypesTable();
    long actualTableSize =
        allTypesTable.queryExecution().analyzed().stats().sizeInBytes().longValue();
    assertThat(actualTableSize).isEqualTo(TestConstants.ALL_TYPES_TABLE_SIZE);
  }

  @Test
  public void testKnownSchema() {
    Dataset<Row> allTypesTable = readAllTypesTable();
    assertThat(allTypesTable.schema()).isEqualTo(allTypesTableSchema);
  }

  @Test
  public void testUserDefinedSchema() {
    assumeTrue("user provided schema is not allowed for this connector", userProvidedSchemaAllowed);
    // TODO(pmkc): consider a schema that wouldn't cause cast errors if read.
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
    assertThat(expectedSchema).isEqualTo(table.schema());
  }

  @Test
  public void testNonExistentSchema() {
    assertThrows(
        "Trying to read a non existing table should throw an exception",
        RuntimeException.class,
        () -> {
          spark.read().format("bigquery").option("table", NON_EXISTENT_TABLE).load();
        });
  }

  @Test(timeout = 10_000) // 10 seconds
  public void testHeadDoesNotTimeoutAndOOM() {
    spark
        .read()
        .format("bigquery")
        .option("table", LARGE_TABLE)
        .load()
        .select(LARGE_TABLE_FIELD)
        .head();
  }

  @Test
  public void testUnhandledFilterOnStruct() {
    // Test fails on Spark 4.0.0
    Assume.assumeThat(spark.version(), CoreMatchers.startsWith("3."));
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data:samples.github_nested")
            .option("filter", "url like '%spark'")
            .load();

    List<Row> result = df.select("url").where("repository is not null").collectAsList();

    assertThat(result).hasSize(85);
  }

  @Test
  public void testQueryMaterializedView() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data:bigqueryml_ncaa.cume_games_view")
            .option("viewsEnabled", "true")
            .load();

    assertThat(df.count()).isGreaterThan(1);
  }

  @Test
  public void testOrAcrossColumnsAndFormats() {
    List<Row> avroResults =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("filter", "word_count = 1 OR corpus_date = 0")
            .option("readDataFormat", "AVRO")
            .load()
            .collectAsList();

    List<Row> arrowResults =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "ARROW")
            .load()
            .where("word_count = 1 OR corpus_date = 0")
            .collectAsList();

    assertThat(avroResults).isEqualTo(arrowResults);
  }

  @Test
  public void testArrowResponseCompressionCodec() {
    List<Row> avroResultsUncompressed =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("filter", "word_count = 1 OR corpus_date = 0")
            .option("readDataFormat", "AVRO")
            .load()
            .collectAsList();

    List<Row> arrowResultsUncompressed =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "ARROW")
            .load()
            .where("word_count = 1 OR corpus_date = 0")
            .collectAsList();

    List<Row> arrowResultsWithLZ4ResponseCompression =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "ARROW")
            .option("responseCompressionCodec", "RESPONSE_COMPRESSION_CODEC_LZ4")
            .load()
            .where("word_count = 1 OR corpus_date = 0")
            .collectAsList();

    assertThat(avroResultsUncompressed).isEqualTo(arrowResultsWithLZ4ResponseCompression);
    assertThat(arrowResultsUncompressed).isEqualTo(arrowResultsWithLZ4ResponseCompression);
  }

  @Test
  public void testAvroResponseCompressionCodec() {
    List<Row> arrowResultsUncompressed =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("filter", "word_count = 1 OR corpus_date = 0")
            .option("readDataFormat", "ARROW")
            .load()
            .collectAsList();

    List<Row> avroResultsUncompressed =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "AVRO")
            .load()
            .where("word_count = 1 OR corpus_date = 0")
            .collectAsList();

    List<Row> avroResultsWithLZ4ResponseCompression =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "AVRO")
            .option("responseCompressionCodec", "RESPONSE_COMPRESSION_CODEC_LZ4")
            .load()
            .where("word_count = 1 OR corpus_date = 0")
            .collectAsList();

    assertThat(arrowResultsUncompressed).isEqualTo(avroResultsWithLZ4ResponseCompression);
    assertThat(avroResultsUncompressed).isEqualTo(avroResultsWithLZ4ResponseCompression);
  }

  @Test
  public void testArrowCompressionCodec() {
    List<Row> avroResults =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("filter", "word_count = 1 OR corpus_date = 0")
            .option("readDataFormat", "AVRO")
            .load()
            .collectAsList();

    List<Row> arrowResultsForZstdCodec =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "ARROW")
            .option("arrowCompressionCodec", "ZSTD")
            .load()
            .where("word_count = 1 OR corpus_date = 0")
            .collectAsList();

    assertThat(avroResults).isEqualTo(arrowResultsForZstdCodec);

    List<Row> arrowResultsForLZ4FrameCodec =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", "ARROW")
            .option("arrowCompressionCodec", "LZ4_FRAME")
            .load()
            .where("word_count = 1 OR corpus_date = 0")
            .collectAsList();

    assertThat(avroResults).isEqualTo(arrowResultsForLZ4FrameCodec);
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

  @Test
  public void testReadFromBigLakeTable_csv() {
    testBigLakeTable(FormatOptions.csv(), TestConstants.SHAKESPEARE_CSV_FILENAME, "text/csv");
  }

  @Test
  public void testReadFromBigLakeTable_json() {
    testBigLakeTable(
        FormatOptions.json(), TestConstants.SHAKESPEARE_JSON_FILENAME, "application/json");
  }

  @Test
  public void testReadFromBigLakeTable_parquet() {
    testBigLakeTable(
        FormatOptions.parquet(),
        TestConstants.SHAKESPEARE_PARQUET_FILENAME,
        "application/octet-stream");
  }

  @Test
  public void testReadFromBigLakeTable_avro() {
    testBigLakeTable(
        FormatOptions.avro(), TestConstants.SHAKESPEARE_AVRO_FILENAME, "application/octet-stream");
  }

  private void testBigLakeTable(FormatOptions formatOptions, String dataFileName, String mimeType) {
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
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset.toString())
            .option("table", table)
            .load();
    testShakespeare(df);
  }

  @Test
  public void testReadFromTableSnapshot() {
    String snapshot =
        String.format("%s.%s.%s_snapshot", TestConstants.PROJECT_ID, testDataset, testTable);
    String allTypes =
        String.format(
            "%s.%s.%s", TestConstants.PROJECT_ID, testDataset, TestConstants.ALL_TYPES_TABLE_NAME);
    IntegrationTestUtils.runQuery(
        String.format("CREATE SNAPSHOT TABLE `%s` CLONE `%s`", snapshot, allTypes));
    Row[] allTypesRows =
        (Row[])
            spark
                .read()
                .format("bigquery")
                .option("dataset", testDataset.toString())
                .option("table", allTypes)
                .load()
                .collect();
    Row[] snapshotRows =
        (Row[])
            spark
                .read()
                .format("bigquery")
                .option("dataset", testDataset.toString())
                .option("table", snapshot)
                .load()
                .collect();
    assertThat(snapshotRows).isEqualTo(allTypesRows);
  }

  @Test
  public void testReadFromTableWithSpacesInName() {
    String tableNameWithSpaces = testTable + " with spaces";

    String tableIdForBqSql =
        String.format(
            "`%s`.`%s`.`%s`",
            TestConstants.PROJECT_ID, testDataset.toString(), tableNameWithSpaces);
    try {
      IntegrationTestUtils.runQuery(
          String.format(
              "CREATE TABLE %s (id INT64, data STRING) AS "
                  + "SELECT * FROM UNNEST([(1, 'foo'), (2, 'bar'), (3, 'baz')])",
              tableIdForBqSql));

      String tableIdForConnector =
          String.format(
              "%s:%s.%s", TestConstants.PROJECT_ID, testDataset.toString(), tableNameWithSpaces);

      Dataset<Row> df = spark.read().format("bigquery").load(tableIdForConnector);

      StructType expectedSchema =
          new StructType()
              .add("id", DataTypes.LongType, true)
              .add("data", DataTypes.StringType, true);

      assertThat(df.schema()).isEqualTo(expectedSchema);
      assertThat(df.count()).isEqualTo(3);

      Dataset<Row> filteredDf =
          spark.read().format("bigquery").option("filter", "id > 1").load(tableIdForConnector);

      assertThat(filteredDf.count()).isEqualTo(2);

      List<Row> rows = filteredDf.sort("id").collectAsList();
      assertThat(rows.get(0).getLong(0)).isEqualTo(2);
      assertThat(rows.get(0).getString(1)).isEqualTo("bar");

    } finally {
      IntegrationTestUtils.runQuery(String.format("DROP TABLE IF EXISTS %s", tableIdForBqSql));
    }
  }

  /**
   * Setting the CreateReadSession timeout to 1000 seconds, which should create the read session
   * since the timeout is more and data is less
   */
  @Test
  public void testCreateReadSessionTimeout() {
    assertThat(
            spark
                .read()
                .format("bigquery")
                .option("table", TestConstants.SHAKESPEARE_TABLE)
                .option("createReadSessionTimeoutInSeconds", 1000)
                .load()
                .collectAsList()
                .size())
        .isEqualTo(TestConstants.SHAKESPEARE_TABLE_NUM_ROWS);
  }

  /**
   * Setting the CreateReadSession timeout to 1 second, to read the
   * `bigquery-public-data.wikipedia.pageviews_2021` table. Should throw run time,
   * DeadlineExceededException
   */
  @Test
  @Ignore("Test has become flaky")
  public void testCreateReadSessionTimeoutWithLessTimeOnHugeData() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", TestConstants.PUBLIC_DATA_WIKIPEDIA_PAGEVIEWS_2021)
            .option("createReadSessionTimeoutInSeconds", 1)
            .option("preferredMinParallelism", "9000")
            .load();
    assertThrows(
        "DEADLINE_EXCEEDED: deadline exceeded ",
        com.google.api.gax.rpc.DeadlineExceededException.class,
        () -> {
          df.where(
                  "views>1000 AND title='Google' AND DATE(datehour) BETWEEN DATE('2021-01-01') AND"
                      + " DATE('2021-07-01')")
              .collect();
        });
  }

  @Test
  public void testNestedFieldProjection() throws Exception {
    Dataset<Row> githubNestedDF =
        spark.read().format("bigquery").load("bigquery-public-data:samples.github_nested");
    List<Row> repositoryUrlRows =
        githubNestedDF
            .filter(
                "repository.has_downloads = true AND url ="
                    + " 'https://github.com/googleapi/googleapi'")
            .select("repository.url")
            .collectAsList();
    assertThat(repositoryUrlRows).hasSize(4);
    Set<String> uniqueUrls =
        repositoryUrlRows.stream().map(row -> row.getString(0)).collect(Collectors.toSet());
    assertThat(uniqueUrls).hasSize(1);
    assertThat(uniqueUrls).contains("https://github.com/googleapi/googleapi");
  }

  @Test
  public void testReadFilteredTimestampField() {
    TimeZone.setDefault(TimeZone.getTimeZone("PST"));
    spark.conf().set("spark.sql.session.timeZone", "UTC");
    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE `%s.%s` AS\n" + "SELECT TIMESTAMP(\"2023-01-09 10:00:00\") as eventTime;",
            testDataset, testTable));
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset.toString())
            .option("table", testTable)
            .load();
    Dataset<Row> filteredDF =
        df.where(df.apply("eventTime").between("2023-01-09 10:00:00", "2023-01-09 10:00:00"));
    List<Row> result = filteredDF.collectAsList();
    assertThat(result).hasSize(1);
    Row head = result.get(0);
    assertThat(head.get(head.fieldIndex("eventTime")))
        .isEqualTo(Timestamp.valueOf("2023-01-09 02:00:00"));
  }

  @Test
  public void testPushDateTimePredicate() {
    IntegrationTestUtils.runQuery(
        String.format(
            "CREATE TABLE `%s.%s` (%s INTEGER, %s DATETIME) "
                + "AS SELECT * FROM UNNEST([(1, DATETIME '2023-09-25 1:00:00'), "
                + "(2, DATETIME '2023-09-29 10:00:00'), (3, DATETIME '2023-10-30 17:30:00')])",
            testDataset, testTable, "orderId", "orderDateTime"));
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset.toString())
            .option("table", testTable)
            .load()
            .where("orderDateTime < '2023-10-25 10:00:00'");
    assertThat(df.count()).isEqualTo(2);
  }

  @Test
  public void testPseudoColumnsRuntimeFilteringDate() {
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
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset.toString())
            .option("table", testTable)
            .option("filter", "_PARTITIONDATE = '" + dt + "'")
            .load()
            .select("orderId");
    List<Row> rows = df.join(df, "orderId").orderBy("orderId").collectAsList();
    assertThat(rows.size()).isEqualTo(2);
    assertThat(rows.get(0).getLong(0)).isEqualTo(101);
    assertThat(rows.get(1).getLong(0)).isEqualTo(102);
  }

  @Test
  public void testPseudoColumnsRuntimeFilteringHour() {
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
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset.toString())
            .option("table", testTable)
            .option("filter", "_PARTITIONTIME = '" + dt + "'")
            .load()
            .select("orderId");
    List<Row> rows = df.join(df, "orderId").orderBy("orderId").collectAsList();
    assertThat(rows.size()).isEqualTo(2);
    assertThat(rows.get(0).getLong(0)).isEqualTo(101);
    assertThat(rows.get(1).getLong(0)).isEqualTo(102);
  }
}
