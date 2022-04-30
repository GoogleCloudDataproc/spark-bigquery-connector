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

import static com.google.cloud.spark.bigquery.integration.IntegrationTestUtils.metadata;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Ignore;
import org.junit.Test;

public class ReadIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

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
  private static final String STRUCT_COLUMN_ORDER_TEST_TABLE_NAME = "struct_column_order";
  private static final String ALL_TYPES_TABLE_NAME = "all_types";
  private static final String ALL_TYPES_VIEW_NAME = "all_types_view";

  /** Generate a test to verify that the given DataFrame is equal to a known result. */
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
  public void testAggregation() {
    // spark.range(1, 100).createOrReplaceTempView("t1");
    // Dataset<Row> df = spark.sql("select id from t1 where t1.id = 10");
    // df.explain(true);

    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", "google.com:hadoop-cloud-dev:vinaylondhe_test.roster")
            .option("materializationDataset", testDataset.toString())
            .load()
            .where("_SchoolID >= 51 and _SchoolID <= 75");

    // df.show();
    df.groupBy("_SchoolID").sum().show();
    //
    // df.groupBy("LastName").sum("_SchoolID").show();
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
    assertThat(allTypesTable.schema()).isEqualTo(TestConstants.ALL_TYPES_TABLE_SCHEMA);
  }

  @Test
  public void testUserDefinedSchema() {
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
  public void testUnhandleFilterOnStruct() {
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
            .option("table", "bigquery-public-data:ethereum_blockchain.live_logs")
            .option("viewsEnabled", "true")
            .option("viewMaterializationProject", PROJECT_ID)
            .option("viewMaterializationDataset", testDataset.toString())
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
}
