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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.integration.model.Data;
import com.google.cloud.spark.bigquery.integration.model.Friend;
import com.google.cloud.spark.bigquery.integration.model.Link;
import com.google.cloud.spark.bigquery.integration.model.Person;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Ignore;
import org.junit.Test;
import scala.Some;

class WriteIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

  private static final String TEMPORARY_GCS_BUCKET_ENV_VARIABLE = "TEMPORARY_GCS_BUCKET";
  private static final String LIBRARIES_PROJECTS_TABLE = "bigquery-public-data.libraries_io.projects";
  private static final String ALL_TYPES_TABLE_NAME = "all_types";
  protected static AtomicInteger id = new AtomicInteger(0);
  protected BigQuery bq;


  protected String temporaryGcsBucket = Preconditions.checkNotNull(
      System.getenv(TEMPORARY_GCS_BUCKET_ENV_VARIABLE),
      "Please set the %s env variable to point to a write enabled GCS bucket",
      TEMPORARY_GCS_BUCKET_ENV_VARIABLE);

  public WriteIntegrationTestBase() {
    super();
    this.bq = BigQueryOptions.getDefaultInstance().getService();
  }

  private Metadata metadata(String key, String value) {
    return metadata(ImmutableMap.of(key, value));
  }

  private Metadata metadata(Map<String, String> map) {
    MetadataBuilder metadata = new MetadataBuilder();
    map.forEach((key, value) -> metadata.putString(key, value));
    return metadata.build();
  }

  @Before
  public void createTestTableName() {
    // have a fresh table for each test
    this.testTable = "test_" + System.nanoTime();
  }

  // Write tests. We have four save modes: Append, ErrorIfExists, Ignore and
  // Overwrite. For each there are two behaviours - the table exists or not.
  // See more at http://spark.apache.org/docs/2.3.2/api/java/org/apache/spark/sql/SaveMode.html

  protected Dataset<Row> initialData() {
    return spark.createDataset(Arrays.asList( //
        new Person("Abc", Arrays.asList(
            new Friend(10, Arrays.asList(new Link("www.abc.com"))))), //
        new Person("Def", Arrays.asList(new Friend(12, Arrays.asList(new Link("www.def.com")))))),
        Encoders.bean(Person.class)).toDF();
  }

  protected Dataset<Row> additonalData() {
    return spark.createDataset(Arrays.asList( //
        new Person("Xyz",
            Arrays.asList(new Friend(10, Arrays.asList(new Link("www.xyz.com"))))),
        new Person("Pqr",
            Arrays.asList(new Friend(12, Arrays.asList(new Link("www.pqr.com")))))),
        Encoders.bean(Person.class)).toDF();
  }

  // getNumRows returns BigInteger, and it messes up the matchers
  protected int testTableNumberOfRows() {
    return bq.getTable(testDataset.toString(), testTable).getNumRows().intValue();
  }

  private StandardTableDefinition testPartitionedTableDefinition() {
    return bq.getTable(testDataset.toString(), testTable + "_partitioned")
        .getDefinition();
  }

  protected void writeToBigQuery(Dataset<Row> df, SaveMode mode) {
    writeToBigQuery(df, mode, "parquet");
  }

  protected void writeToBigQuery(Dataset<Row> df, SaveMode mode, String format) {
    df.write().format("bigquery")
        .mode(mode)
        .option("table", fullTableName())
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("intermediateFormat", format)
        .save();
  }

  Dataset<Row> readAllTypesTable() {
    return spark.read().format("bigquery")
        .option("dataset", testDataset.toString())
        .option("table", ALL_TYPES_TABLE_NAME)
        .load();
  }

  @Test
  public void testWriteToBigQuery_AppendSaveMode() {
    // initial write
    writeToBigQuery(initialData(), SaveMode.Append);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    // second write
    writeToBigQuery(additonalData(), SaveMode.Append);
    assertThat(testTableNumberOfRows()).isEqualTo(4);
    assertThat(additionalDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQuery_ErrorIfExistsSaveMode() {
    // initial write
    writeToBigQuery(initialData(), SaveMode.ErrorIfExists);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    // second write
    assertThrows(IllegalArgumentException.class, () -> {
      writeToBigQuery(additonalData(), SaveMode.ErrorIfExists);
    });
  }

  @Test
  public void testWriteToBigQuery_IgnoreSaveMode() {
    // initial write
    writeToBigQuery(initialData(), SaveMode.Ignore);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    // second write
    writeToBigQuery(additonalData(), SaveMode.Ignore);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    assertThat(additionalDataValuesExist()).isFalse();
  }

  @Test
  public void testWriteToBigQuery_OverwriteSaveMode() {
    // initial write
    writeToBigQuery(initialData(), SaveMode.Overwrite);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    // second write
    writeToBigQuery(additonalData(), SaveMode.Overwrite);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isFalse();
    assertThat(additionalDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQuery_AvroFormat() {
    writeToBigQuery(initialData(), SaveMode.ErrorIfExists, "avro");
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }


  @Test
  public void testWriteToBigQuerySimplifiedApi() {
    initialData().write().format("bigquery")
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .save(fullTableName());
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  //   test("write all types to bq - avro format() {
  //
  //     // temporarily skipping for v1, as "AVRO" write format is throwing error
  //     // while writing to GCS
  //     if("bigquery".equals("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")) {
  //       val allTypesTable = readAllTypesTable("bigquery")
  //       writeToBigQuery(allTypesTable, SaveMode.Overwrite, "avro")
  //
  //       val df = spark.read.format("bigquery")
  //         .option("dataset", testDataset.toString())
  //         .option("table", testTable)
  //         .load()
  //
  //       compareBigNumericDataSetRows(df.head(), allTypesTable.head())
  //       compareBigNumericDataSetSchema(df.schema, allTypesTable.schema)
  //     }
  //   }

  @Test
  public void testWriteToBigQueryAddingTheSettingsToSparkConf() {
    spark.conf().set("temporaryGcsBucket", temporaryGcsBucket);
    initialData().write().format("bigquery")
        .option("table", fullTableName())
        .save();
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQueryPartitionedAndClusteredTable() {
    Dataset<Row> df = spark.read().format("bigquery")
        .option("table", LIBRARIES_PROJECTS_TABLE)
        .load()
        .where("platform = 'Sublime'");

    df.write().format("bigquery")
        .option("table", fullTableNamePartitioned())
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("partitionField", "created_timestamp")
        .option("clusteredFields", "platform")
        .mode(SaveMode.Overwrite)
        .save();

    StandardTableDefinition tableDefinition = testPartitionedTableDefinition();
    assertThat(tableDefinition.getTimePartitioning().getField()).isEqualTo("created_timestamp");
    assertThat(tableDefinition.getClustering().getFields()).contains("platform");
  }

  protected Dataset<Row> overwriteSinglePartition(StructField dateField) {
    // create partitioned table
    String tableName = fullTableNamePartitioned() + "_" + id.getAndIncrement();
    TableId tableId = TableId.of(testDataset.toString(), tableName);
    Schema schema = Schema.of(
        Field.of("the_date", LegacySQLTypeName.DATE),
        Field.of("some_text", LegacySQLTypeName.STRING)
    );
    TimePartitioning timePartitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
        .setField("the_date").build();
    StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
        .setSchema(schema)
        .setTimePartitioning(timePartitioning)
        .build();
    bq.create(TableInfo.of(tableId, tableDefinition));
    // entering the data
    try {
      bq.query(QueryJobConfiguration.of(
          String.format("insert into `" + fullTableName()
              + "` (the_date, some_text) values ('2020-07-01', 'foo'), ('2020-07-02', 'bar')")));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // overrding a single partition
    List<Row> rows = Arrays.asList(RowFactory.create(Date.valueOf("2020-07-01"), "baz"));
    StructType newDataSchema = new StructType(new StructField[]{
        dateField,
        new StructField("some_text", DataTypes.StringType, true, Metadata.empty())});
    Dataset<Row> newDataDF = spark.createDataFrame(rows, newDataSchema);

    newDataDF.write().format("bigquery")
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("datePartition", "20200701")
        .mode("overwrite")
        .save(fullTableName());

    Dataset<Row> resultDF = spark.read().format("bigquery").load(fullTableName());
    List<Row> result = resultDF.collectAsList();

    assertThat(result).hasSize(2);
    assertThat(result.stream().filter(row -> row.getString(1).equals("bar")).count()).isEqualTo(1);
    assertThat(result.stream().filter(row -> row.getString(1).equals("baz")).count()).isEqualTo(1);

    return resultDF;
  }

  //TODO: figure out why the table creation fails
  //@Test
  public void testOverwriteSinglePartition() {
    overwriteSinglePartition(
        new StructField("the_date", DataTypes.DateType, true, Metadata.empty()));
  }

  //TODO: figure out why the table creation fails
  //@Test
  public void testOverwriteSinglePartitionWithComment() {
    String comment = "the partition field";
    Dataset<Row> resultDF = overwriteSinglePartition(
        new StructField("the_date", DataTypes.DateType, true, Metadata.empty())
            .withComment(comment));
    assertThat(resultDF.schema().fields()[0].getComment()).isEqualTo(Some.apply(comment));
  }

  //   //    test("support custom data types() {
  //   //      val table = s"$testDataset.toString().$testTable"
  //   //
  //   //      val originalVectorDF = spark.createDataFrame(
  //   //        List(Row("row1", 1, Vectors.dense(1, 2, 3))).asJava,
  //   //        StructType(Seq(
  //   //          StructField("name", DataTypes.StringType),
  //   //          StructField("num", DataTypes.IntegerType),
  //   //          StructField("vector", SQLDataTypes.VectorType))))
  //   //
  //   //      originalVectorDF.write.format("bigquery")
  //   //        // must use avro or orc
  //   //        .option("intermediateFormat", "avro")
  //   //        .option("temporaryGcsBucket", temporaryGcsBucket)
  //   //        .save(table)
  //   //
  //   //      val readVectorDF = spark.read.format("bigquery")
  //   //        .load(table)
  //   //
  //   //      val orig = originalVectorDF.head
  //   //      val read = readVectorDF.head
  //   //
  //   //      read should equal(orig)
  //   //    }

  @Test
  public void testWriteToBigQueryWithDescription() {
    String testDescription = "test description";
    String testComment = "test comment";

    Metadata metadata =
        Metadata.fromJson("{\"description\": \"" + testDescription + "\"}");

    StructType[] schemas = new StructType[]{
        structType(new StructField("c1", DataTypes.IntegerType, true, metadata)),
        structType(
            new StructField("c1", DataTypes.IntegerType, true, Metadata.empty())
                .withComment(testComment)),
        structType(
            new StructField("c1", DataTypes.IntegerType, true, metadata)
                .withComment(testComment)),
        structType(new StructField("c1", DataTypes.IntegerType, true, Metadata.empty()))
    };

    String[] readValues = new String[]{testDescription, testComment, testComment, null};

    for (int i = 0; i < schemas.length; i++) {
      List<Row> data = Arrays.asList(RowFactory.create(100), RowFactory.create(200));

      Dataset<Row> descriptionDF = spark.createDataFrame(data, schemas[i]);

      writeToBigQuery(descriptionDF, SaveMode.Overwrite);

      Dataset<Row> readDF = spark.read().format("bigquery")
          .option("dataset", testDataset.toString())
          .option("table", testTable)
          .load();

      Optional<String> description =
          SchemaConverters.getDescriptionOrCommentOfField(readDF.schema().fields()[0]);

      if (readValues[i] != null) {
        assertThat(description.isPresent()).isTrue();
        assertThat(description.orElse("")).isEqualTo(readValues[i]);
      } else {
        assertThat(description.isPresent()).isFalse();
      }
    }
  }


  private StructType structType(StructField... fields) {
    return new StructType(fields);
  }


  @Test
  public void testPartition_Hourly() {
    testPartition("HOUR");
  }

  @Test
  public void testPartition_Daily() {
    testPartition("DAY");
  }

  @Test
  public void testPartition_Monthly() {
    testPartition("MONTH");
  }

  @Test
  public void testPartition_Yearly() {
    testPartition("YEAR");
  }

  private void testPartition(String partitionType) {
    List<Data> data = Arrays.asList(
        new Data("a", Timestamp.valueOf("2020-01-01 01:01:01")),
        new Data("b", Timestamp.valueOf("2020-01-02 02:02:02")),
        new Data("c", Timestamp.valueOf("2020-01-03 03:03:03"))
    );
    Dataset<Row> df = spark.createDataset(data, Encoders.bean(Data.class)).toDF();
    String table = testDataset.toString() + "." + testTable + "_" + partitionType;
    df.write().format("bigquery")
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("partitionField", "ts")
        .option("partitionType", partitionType)
        .option("partitionRequireFilter", "true")
        .option("table", table)
        .save();

    Dataset<Row> readDF = spark.read().format("bigquery").load(table);
    assertThat(readDF.count()).isEqualTo(3);
  }

  @Test
  public void testCacheDataFrameInDataSource() {
    Dataset<Row> allTypesTable = readAllTypesTable();
    writeToBigQuery(allTypesTable, SaveMode.Overwrite, "avro");

    Dataset<Row> df = spark.read().format("bigquery")
        .option("dataset", testDataset.toString())
        .option("table", testTable)
        .option("readDataFormat", "arrow")
        .load().cache();

    assertThat(df.head()).isEqualTo(allTypesTable.head());

    // read from cache
    assertThat(df.head()).isEqualTo(allTypesTable.head());
    assertThat(df.schema()).isEqualTo(allTypesTable.schema());
  }


  protected long numberOfRowsWith(String name) {
    try {
      return bq.query(QueryJobConfiguration
          .of(String.format("select name from %s where name='%s'", fullTableName(), name)))
          .getTotalRows();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected String fullTableName() {
    return testDataset.toString() + "." + testTable;
  }

  protected String fullTableNamePartitioned() {
    return fullTableName() + "_partitioned";
  }

  protected boolean additionalDataValuesExist() {
    return numberOfRowsWith("Xyz") == 1;
  }

  protected boolean initialDataValuesExist() {
    return numberOfRowsWith("Abc") == 1;
  }

}

