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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Ignore;
import org.junit.Test;


public class ReadByFormatIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

  private static final Map<String, Collection<String>> FILTER_DATA = ImmutableMap.<String, Collection<String>>builder()
      .put("word_count == 4", ImmutableList.of("'A", "'But", "'Faith"))
      .put("word_count > 3", ImmutableList.of("'", "''Tis", "'A"))
      .put("word_count >= 2", ImmutableList.of("'", "''Lo", "''O"))
      .put("word_count < 3", ImmutableList.of("''All", "''Among", "''And"))
      .put("word_count <= 5", ImmutableList.of("'", "''All", "''Among"))
      .put("word_count in(8, 9)", ImmutableList.of("'", "'Faith", "'Tis"))
      .put("word_count is null", ImmutableList.of())
      .put("word_count is not null", ImmutableList.of("'", "''All", "''Among"))
      .put("word_count == 4 and corpus == 'twelfthnight'", ImmutableList.of("'Thou", "'em", "Art"))
      .put("word_count == 4 or corpus > 'twelfthnight'", ImmutableList.of("'", "''Tis", "''twas"))
      .put("not word_count in(8, 9)", ImmutableList.of("'", "''All", "''Among"))
      .put("corpus like 'king%'", ImmutableList.of("'", "'A", "'Affectionate"))
      .put("corpus like '%kinghenryiv'", ImmutableList.of("'", "'And", "'Anon"))
      .put("corpus like '%king%'", ImmutableList.of("'", "'A", "'Affectionate"))
      .build();
  private static final String LIBRARIES_PROJECTS_TABLE = "bigquery-public-data.libraries_io.projects";
  private static final String SHAKESPEARE_TABLE = "bigquery-public-data.samples.shakespeare";
  private static final long SHAKESPEARE_TABLE_NUM_ROWS = 164656L;
  private static final StructType SHAKESPEARE_TABLE_SCHEMA = new StructType(new StructField[]{
      StructField.apply("word", DataTypes.StringType, false, metadata("description",
          "A single unique word (where whitespace is the delimiter) extracted from a corpus.")),
      StructField.apply("word_count", DataTypes.LongType, false, metadata("description",
          "The number of times this word appears in this corpus.")),
      StructField.apply("corpus", DataTypes.StringType, false, metadata("description",
          "The work from which this word was extracted.")),
      StructField.apply("corpus_date", DataTypes.LongType, false, metadata("description",
          "The year in which this corpus was published."))});


  private static final StructType SHAKESPEARE_TABLE_SCHEMA_WITH_METADATA_COMMENT = new StructType(
      Stream.of(SHAKESPEARE_TABLE_SCHEMA.fields()).map(
          field -> {
            Metadata metadata =
                new MetadataBuilder()
                    .withMetadata(field.metadata())
                    .putString("comment", field.metadata().getString("description"))
                    .build();
            return new StructField(field.name(), field.dataType(), field.nullable(), metadata);
          }
      ).toArray(StructField[]::new));

  private static final String LARGE_TABLE = "bigquery-public-data.samples.natality";
  private static final String LARGE_TABLE_FIELD = "is_male";
  private static final long LARGE_TABLE_NUM_ROWS = 33271914L;
  private static final String NON_EXISTENT_TABLE = "non-existent.non-existent.non-existent";
  private static final String STRUCT_COLUMN_ORDER_TEST_TABLE_NAME = "struct_column_order";
  private static final String ALL_TYPES_TABLE_NAME = "all_types";
  private static final String ALL_TYPES_VIEW_NAME = "all_types_view";
  private String testDataset;
  private String testTable;
  private String dataFormat;

  public ReadByFormatIntegrationTestBase(SparkSession spark,String testDataset, String dataFormat) {
    super(spark);
    this.testDataset = testDataset;
    this.dataFormat = dataFormat;
  }

//
// before {
//   // have a fresh table for each test
//   testTable = s"test_${System.nanoTime()}"
// }
////

// override def beforeAll: Unit = {
//   spark = TestUtils.getOrCreateSparkSession(getClass.getSimpleName)
//   testDataset = s"spark_bigquery_${getClass.getSimpleName}_${System.currentTimeMillis()}"
//   IntegrationTestUtils.createDataset(testDataset)
//   IntegrationTestUtils.runQuery(
//     TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE.format(s"$testDataset.$ALL_TYPES_TABLE_NAME"))
//   IntegrationTestUtils.createView(testDataset, ALL_TYPES_TABLE_NAME, ALL_TYPES_VIEW_NAME)
//   IntegrationTestUtils.runQuery(
//     TestConstants
//       .STRUCT_COLUMN_ORDER_TEST_TABLE_QUERY_TEMPLATE
//       .format(s"$testDataset.$STRUCT_COLUMN_ORDER_TEST_TABLE_NAME"))
// }


// def testsWithReadInFormat(dataSourceFormat: String, dataFormat: String): Unit = {
//
  @Test public void testViewWithDifferentColumnsForSelectAndFilter() {

    Dataset<Row> df = getViewDataFrame();

    // filer and select are pushed down to BQ
    List<Row> result = df
      .select("int_req")
      .filter("str = 'string'")
      .collectAsList();

    assertThat(result).hasSize(1);
    List<Row> filteredResult = result.stream().filter(row -> row.getInt(0) == 42)
        .collect(Collectors.toList());
    assertThat(filteredResult).hasSize(1);
   }

  @Test public void testCachedViewWithDifferentColumnsForSelectAndFilter() {

    Dataset<Row>  df = getViewDataFrame();
    Dataset<Row>  cachedDF = df.cache();

    // filter and select are run on the spark side as the view was cached
    List<Row> result = cachedDF
        .select("int_req")
        .filter("str = 'string'")
        .collectAsList();

    assertThat(result).hasSize(1);
    List<Row> filteredResult = result.stream().filter(row -> row.getInt(0) == 42)
        .collect(Collectors.toList());
    assertThat(filteredResult).hasSize(1);
  }

  @Test public void testOutOfOrderColumns() {
    Row row = spark.read().format("bigquery")
      .option("table", SHAKESPEARE_TABLE)
      .option("readDataFormat", dataFormat).load()
      .select("word_count", "word").head();
    assertThat(row.get(0)).isInstanceOf(Long.class);
    assertThat(row.get(1)).isInstanceOf(String.class);
  }

  @Test public void testSelectAllColumnsFromATable() {
    Row row = spark.read().format("bigquery")
      .option("table", SHAKESPEARE_TABLE)
      .option("readDataFormat", dataFormat).load()
      .select("word_count", "word", "corpus", "corpus_date").head();
    assertThat(row.get(0)).isInstanceOf(Long.class);
    assertThat(row.get(1)).isInstanceOf(String.class);
    assertThat(row.get(2)).isInstanceOf(String.class);
    assertThat(row.get(3)).isInstanceOf(Long.class);
  }

//   //    @Test public void testcache data frame in DataSource %s. Data Format %s"
//   //    .format(dataSourceFormat, dataFormat)) {
//   //      val allTypesTable = readAllTypesTable("bigquery")
//   //      writeToBigQuery(allTypesTable, SaveMode.Overwrite, "avro")
//   //
//   //      val df = spark.read.format("bigquery")
//   //        .option("dataset", testDataset)
//   //        .option("table", testTable)
//   //        .option("readDataFormat", "arrow")
//   //        .load().cache()
//   //
//   //      assertThat(df.head() == allTypesTable.head())
//   //
//   //      // read from cache
//   //      assertThat(df.head() == allTypesTable.head())
//   //      assertThat(df.schema == allTypesTable.schema)
//   //    }
//
  @Test public void testNumberOfPartitions() {
    Dataset<Row> df = spark.read().format("bigquery")
      .option("table", LARGE_TABLE)
      .option("parallelism", "5")
      .option("readDataFormat", dataFormat)
      .load();
    assertThat(df.rdd().getNumPartitions()).isEqualTo(5);
  }

//   @Test public void testdefault number of partitions. DataSource %s. Data Format %s"
//     .format(dataSourceFormat, dataFormat)) {
//     val df = spark.read.format(dataSourceFormat)
//       .option("table", LARGE_TABLE)
//       .option("readDataFormat", dataFormat)
//       .load()
//
//     assertThat(df.rdd.getNumPartitions == 58)
//   }
//
//   @Test public void testbalanced partitions. DataSource %s. Data Format %s"
//     .format(dataSourceFormat, dataFormat)) {
//     import com.google.cloud.spark.bigquery._
//     failAfter(120 seconds) {
//       // Select first partition
//       val df = spark.read
//         .option("parallelism", 5)
//         .option("readDataFormat", dataFormat)
//         .option("filter", "year > 2000")
//         .bigquery(LARGE_TABLE)
//         .select(LARGE_TABLE_FIELD) // minimize payload
//       val sizeOfFirstPartition = df.rdd.mapPartitionsWithIndex {
//         case (_, it) => Iterator(it.size)
//       }.collect().head
//
//       // Since we are only reading from a single stream, we can expect to get
//       // at least as many rows
//       // in that stream as a perfectly uniform distribution would command.
//       // Note that the assertion
//       // is on a range of rows because rows are assigned to streams on the
//       // server-side in
//       // indivisible units of many rows.
//
//       val numRowsLowerBound = LARGE_TABLE_NUM_ROWS / df.rdd.getNumPartitions
//       assertThat(numRowsLowerBound <= sizeOfFirstPartition &&
//         sizeOfFirstPartition < (numRowsLowerBound * 1.1).toInt)
//     }
//   }
//
//   @Test public void testtest optimized count(*). DataSource %s. Data Format %s"
//     .format(dataSourceFormat, dataFormat)) {
//     DirectBigQueryRelation.emptyRowRDDsCreated = 0
//     val oldMethodCount = spark.read.format(dataSourceFormat)
//       .option("table", "bigquery-public-data.samples.shakespeare")
//       .option("optimizedEmptyProjection", "false")
//       .option("readDataFormat", dataFormat)
//       .load()
//       .select("corpus_date")
//       .where("corpus_date > 0")
//       .count()
//
//     assertThat(DirectBigQueryRelation.emptyRowRDDsCreated == 0)
//
//     assertResult(oldMethodCount) {
//       spark.read.format(dataSourceFormat)
//         .option("table", "bigquery-public-data.samples.shakespeare")
//         .option("readDataFormat", dataFormat)
//         .load()
//         .where("corpus_date > 0")
//         .count()
//     }
//
//     if ("bigquery" == dataSourceFormat) {
//       assertThat(DirectBigQueryRelation.emptyRowRDDsCreated == 1)
//     }
//   }
//
//   @Test public void testtest optimized count(*) with filter. DataSource %s. Data Format %s"
//     .format(dataSourceFormat, dataFormat)) {
//     DirectBigQueryRelation.emptyRowRDDsCreated = 0
//     val oldMethodCount = spark.read.format(dataSourceFormat)
//       .option("table", "bigquery-public-data.samples.shakespeare")
//       .option("optimizedEmptyProjection", "false")
//       .option("readDataFormat", dataFormat)
//       .load()
//       .select("corpus_date")
//       .count()
//
//     assertThat(DirectBigQueryRelation.emptyRowRDDsCreated == 0)
//
//     assertResult(oldMethodCount) {
//       spark.read.format(dataSourceFormat)
//         .option("table", "bigquery-public-data.samples.shakespeare")
//         .option("readDataFormat", dataFormat)
//         .load()
//         .count()
//     }
//     if ("bigquery" == dataSourceFormat) {
//       assertThat(DirectBigQueryRelation.emptyRowRDDsCreated == 1)
//     }
//   }
//
//   @Test public void testkeeping filters behaviour. DataSource %s. Data Format %s"
//     .format(dataSourceFormat, dataFormat)) {
//     val newBehaviourWords = extractWords(
//       spark.read.format(dataSourceFormat)
//         .option("table", "bigquery-public-data.samples.shakespeare")
//         .option("filter", "length(word) = 1")
//         .option("combinePushedDownFilters", "true")
//         .option("readDataFormat", dataFormat)
//         .load())
//
//     val oldBehaviourWords = extractWords(
//       spark.read.format(dataSourceFormat)
//         .option("table", "bigquery-public-data.samples.shakespeare")
//         .option("filter", "length(word) = 1")
//         .option("combinePushedDownFilters", "false")
//         .option("readDataFormat", dataFormat)
//         .load())
//
//     newBehaviourWords should equal(oldBehaviourWords)
//   }
//
//   @Test public void testcolumn order of struct. DataSource %s. Data Format %s"
//     .format(dataSourceFormat, dataFormat)) {
//     val sqlContext = spark.sqlContext
//     val schema = Encoders.bean(classOf[TestConstants.ColumnOrderTestClass]).schema
//
//     val dataset = spark.read
//       .schema(schema)
//       .option("dataset", testDataset)
//       .option("table", STRUCT_COLUMN_ORDER_TEST_TABLE_NAME)
//       .format(dataSourceFormat)
//       .option("readDataFormat", dataFormat)
//       .load()
//       .as(Encoders.bean(classOf[TestConstants.ColumnOrderTestClass]))
//
//     val row = Seq(dataset.head())(0)
//     assertThat(row == TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_COLS)
//   }
// }
//
Dataset<Row> getViewDataFrame() {
  return spark.read().format("bigquery")
      .option("table", ALL_TYPES_VIEW_NAME)
      .option("viewsEnabled", "true")
      .option("viewMaterializationProject", System.getenv("GOOGLE_CLOUD_PROJECT"))
      .option("viewMaterializationDataset", testDataset)
      .option("readDataFormat", dataFormat)
      .load();
}

Dataset<Row> readAllTypesTable() {
  return spark.read().format("bigquery")
    .option("dataset", testDataset)
    .option("table", ALL_TYPES_TABLE_NAME)
    .load();
}

// Seq("bigquery", "com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
//   .foreach(testsWithDataSource)
//
// def testsWithDataSource(dataSourceFormat: String) {
//
//   @Test public void testOR across columns with Arrow. DataSource %s".format(dataSourceFormat)) {
//
//     val avroResults = spark.read.format("bigquery")
//       .option("table", "bigquery-public-data.samples.shakespeare")
//       .option("filter", "word_count = 1 OR corpus_date = 0")
//       .option("readDataFormat", "AVRO")
//       .load().collect()
//
//     val arrowResults = spark.read.format("bigquery")
//       .option("table", "bigquery-public-data.samples.shakespeare")
//       .option("readDataFormat", "ARROW")
//       .load().where("word_count = 1 OR corpus_date = 0")
//       .collect()
//
//     avroResults should equal(arrowResults)
//   }
//
//   // Disabling the test until the merge the master
//   // TODO: enable it
//   /*
//   @Test public void testCount with filters - Arrow. DataSource %s".format(dataSourceFormat)) {
//
//     val countResults = spark.read.format(dataSourceFormat)
//       .option("table", "bigquery-public-data.samples.shakespeare")
//       .option("readDataFormat", "ARROW")
//       .load().where("word_count = 1 OR corpus_date = 0")
//       .count()
//
//     val countAfterCollect = spark.read.format(dataSourceFormat)
//       .option("table", "bigquery-public-data.samples.shakespeare")
//       .option("readDataFormat", "ARROW")
//       .load().where("word_count = 1 OR corpus_date = 0")
//       .collect().size
//
//     countResults should equal(countAfterCollect)
//   }
//   */
//   @Test public void testread data types. DataSource %s".format(dataSourceFormat)) {
//     // temporarily skipping for v2
//     if (dataSourceFormat.equals("bigquery")) {
//       val allTypesTable = readAllTypesTable(dataSourceFormat)
//       val expectedRow = spark.range(1).select(TestConstants.ALL_TYPES_TABLE_COLS: _*).head.toSeq
//       val rows = allTypesTable.head.toSeq
//
//       var i = 0
//       for (row <- rows) {
//
//         if (i == TestConstants.BIG_NUMERIC_COLUMN_POSITION) {
//           for (j <- 0 to 1) {
//             val bigNumericValue =
//               row.asInstanceOf[GenericRowWithSchema].get(j).asInstanceOf[BigNumeric]
//
//             val bigNumericString = bigNumericValue.getNumber.toPlainString
//
//             val expectedBigNumericString =
//               expectedRow(i).asInstanceOf[GenericRowWithSchema].get(j)
//
//             assertThat(bigNumericString === expectedBigNumericString)
//           }
//         } else {
//           assertThat(row === expectedRow(i))
//         }
//
//         i += 1
//       }
//     }
//   }
//
//
//   @Test public void testknown size in bytes. DataSource %s".format(dataSourceFormat)) {
//     val allTypesTable = readAllTypesTable(dataSourceFormat)
//     val actualTableSize = allTypesTable.queryExecution.analyzed.stats.sizeInBytes
//     assertThat(actualTableSize == TestConstants.ALL_TYPES_TABLE_SIZE)
//   }
//
//   @Test public void testknown schema. DataSource %s".format(dataSourceFormat)) {
//     val allTypesTable = readAllTypesTable(dataSourceFormat)
//     assertThat(allTypesTable.schema == TestConstants.ALL_TYPES_TABLE_SCHEMA)
//   }
//
//   @Test public void testuser defined schema. DataSource %s".format(dataSourceFormat)) {
//     // TODO(pmkc): consider a schema that wouldn't cause cast errors if read.
//     val expectedSchema = StructType(Seq(StructField("whatever", ByteType)))
//     val table = spark.read.schema(expectedSchema)
//       .format(dataSourceFormat)
//       .option("table", SHAKESPEARE_TABLE)
//       .load()
//     assertThat(expectedSchema == table.schema)
//   }
//
//   @Test public void testnon-existent schema. DataSource %s".format(dataSourceFormat)) {
//     assertThrows[RuntimeException] {
//       spark.read.format(dataSourceFormat).option("table", NON_EXISTENT_TABLE).load()
//     }
//   }
//
//   @Test public void testhead does not time out and OOM. DataSource %s".format(dataSourceFormat)) {
//     failAfter(10 seconds) {
//       spark.read.format(dataSourceFormat)
//         .option("table", LARGE_TABLE)
//         .load()
//         .select(LARGE_TABLE_FIELD)
//         .head
//     }
//   }
//   @Test public void testUnhandle filter on struct. DataSource %s".format(dataSourceFormat)) {
//     val df = spark.read.format(dataSourceFormat)
//       .option("table", "bigquery-public-data:samples.github_nested")
//       .option("filter", "url like '%spark'")
//       .load()
//
//     val result = df.select("url")
//       .where("repository is not null")
//       .collect()
//
//     result.size shouldBe 85
//   }
// }
//
// // Write tests. We have four save modes: Append, ErrorIfExists, Ignore and
// // Overwrite. For each there are two behaviours - the table exists or not.
// // See more at http://spark.apache.org/docs/2.3.2/api/java/org/apache/spark/sql/SaveMode.html
//
// override def afterAll: Unit = {
//   IntegrationTestUtils.deleteDatasetAndTables(testDataset)
// }
//
//
// def extractWords(df: DataFrame): Set[String] = {
//   df.select("word")
//     .where("corpus_date = 0")
//     .collect()
//     .map(_.getString(0))
//     .toSet
// }

}

