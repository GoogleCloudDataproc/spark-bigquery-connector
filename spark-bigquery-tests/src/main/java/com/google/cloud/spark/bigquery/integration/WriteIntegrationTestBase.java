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

import static com.google.common.truth.Truth.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeThat;

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
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.integration.model.Data;
import com.google.cloud.spark.bigquery.integration.model.Friend;
import com.google.cloud.spark.bigquery.integration.model.Link;
import com.google.cloud.spark.bigquery.integration.model.Person;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.ProvisionException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.SparkException;
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
import org.junit.Ignore;
import org.junit.Test;
import scala.Some;

abstract class WriteIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

  private static final String TEMPORARY_GCS_BUCKET_ENV_VARIABLE = "TEMPORARY_GCS_BUCKET";
  protected static AtomicInteger id = new AtomicInteger(0);
  protected final SparkBigQueryConfig.WriteMethod writeMethod;
  protected BigQuery bq;

  protected String temporaryGcsBucket =
      Preconditions.checkNotNull(
          System.getenv(TEMPORARY_GCS_BUCKET_ENV_VARIABLE),
          "Please set the %s env variable to point to a write enabled GCS bucket",
          TEMPORARY_GCS_BUCKET_ENV_VARIABLE);

  public WriteIntegrationTestBase(SparkBigQueryConfig.WriteMethod writeMethod) {
    super();
    this.writeMethod = writeMethod;
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
    return spark
        .createDataset(
            Arrays.asList(
                new Person(
                    "Abc",
                    Arrays.asList(new Friend(10, Arrays.asList(new Link("www.abc.com"))))), //
                new Person(
                    "Def", Arrays.asList(new Friend(12, Arrays.asList(new Link("www.def.com")))))),
            Encoders.bean(Person.class))
        .toDF();
  }

  protected Dataset<Row> additonalData() {
    return spark
        .createDataset(
            Arrays.asList(
                new Person(
                    "Xyz", Arrays.asList(new Friend(10, Arrays.asList(new Link("www.xyz.com"))))),
                new Person(
                    "Pqr", Arrays.asList(new Friend(12, Arrays.asList(new Link("www.pqr.com")))))),
            Encoders.bean(Person.class))
        .toDF();
  }

  protected int testTableNumberOfRows() throws InterruptedException {
    String query = String.format("select * from %s.%s", testDataset.toString(), testTable);
    return (int) bq.query(QueryJobConfiguration.of(query)).getTotalRows();
  }

  private StandardTableDefinition testPartitionedTableDefinition() {
    return bq.getTable(testDataset.toString(), testTable + "_partitioned").getDefinition();
  }

  protected void writeToBigQuery(Dataset<Row> df, SaveMode mode) {
    writeToBigQuery(df, mode, "parquet");
  }

  protected void writeToBigQuery(Dataset<Row> df, SaveMode mode, String format) {
    df.write()
        .format("bigquery")
        .mode(mode)
        .option("table", fullTableName())
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("intermediateFormat", format)
        .option("writeMethod", writeMethod.toString())
        .save();
  }

  Dataset<Row> readAllTypesTable() {
    return spark
        .read()
        .format("bigquery")
        .option("dataset", testDataset.toString())
        .option("table", TestConstants.ALL_TYPES_TABLE_NAME)
        .load();
  }

  @Test
  public void testWriteToBigQuery_AppendSaveMode() throws InterruptedException {
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
  public void testWriteToBigQuery_ErrorIfExistsSaveMode() throws InterruptedException {
    // initial write
    writeToBigQuery(initialData(), SaveMode.ErrorIfExists);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    assertThrows(
        IllegalArgumentException.class,
        () -> writeToBigQuery(additonalData(), SaveMode.ErrorIfExists));
  }

  @Test
  public void testWriteToBigQuery_IgnoreSaveMode() throws InterruptedException {
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
  public void testWriteToBigQuery_OverwriteSaveMode() throws InterruptedException {
    // initial write
    writeToBigQuery(initialData(), SaveMode.Overwrite);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();

    // Adding a two minute cushion as the data takes some time to move from buffer to the actual
    // table. Without this cushion, get the following error:
    // "UPDATE or DELETE statement over {DestinationTable} would affect rows in the streaming
    // buffer, which is not supported"
    Thread.sleep(120 * 1000);

    // second write
    writeToBigQuery(additonalData(), SaveMode.Overwrite);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isFalse();
    assertThat(additionalDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQuery_AvroFormat() throws InterruptedException {
    writeToBigQuery(initialData(), SaveMode.ErrorIfExists, "avro");
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQuerySimplifiedApi() throws InterruptedException {
    initialData()
        .write()
        .format("bigquery")
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .save(fullTableName());
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQueryAddingTheSettingsToSparkConf() throws InterruptedException {
    spark.conf().set("temporaryGcsBucket", temporaryGcsBucket);
    initialData()
        .write()
        .format("bigquery")
        .option("table", fullTableName())
        .option("writeMethod", writeMethod.toString())
        .save();
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  @Ignore("DSv2 only")
  public void testDirectWriteToBigQueryWithDiffInSchema() {
    spark.conf().set("temporaryGcsBucket", temporaryGcsBucket);
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", testDataset + "." + TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_NAME)
            .load();

    assertThrows(
        ProvisionException.class,
        () ->
            df.write()
                .format("bigquery")
                .mode(SaveMode.Append)
                .option("writeMethod", "direct")
                .save(testDataset + "." + TestConstants.DIFF_IN_SCHEMA_DEST_TABLE_NAME));
  }

  @Test
  @Ignore("DSv2 only")
  public void testDirectWriteToBigQueryWithDiffInSchemaAndDisableModeCheck() {
    spark.conf().set("temporaryGcsBucket", temporaryGcsBucket);
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", testDataset + "." + TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_NAME)
            .load();

    assertThrows(
        ProvisionException.class,
        () ->
            df.write()
                .format("bigquery")
                .mode(SaveMode.Append)
                .option("writeMethod", "direct")
                .option("enableModeCheckForSchemaFields", false)
                .save(testDataset + "." + TestConstants.DIFF_IN_SCHEMA_DEST_TABLE_NAME));
  }

  @Test
  @Ignore("DSv2 only")
  public void testInDirectWriteToBigQueryWithDiffInSchemaAndModeCheck() {
    spark.conf().set("temporaryGcsBucket", temporaryGcsBucket);
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", testDataset + "." + TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_NAME)
            .load();

    assertThrows(
        SparkException.class,
        () ->
            df.write()
                .format("bigquery")
                .mode(SaveMode.Append)
                .option("writeMethod", "indirect")
                .option("enableModeCheckForSchemaFields", true)
                .save(testDataset + "." + TestConstants.DIFF_IN_SCHEMA_DEST_TABLE_NAME));
  }

  @Test
  public void testIndirectWriteToBigQueryWithDiffInSchemaNullableFieldAndDisableModeCheckk() {
    spark.conf().set("temporaryGcsBucket", temporaryGcsBucket);
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", testDataset + "." + TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_NAME)
            .load();

    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("writeMethod", "indirect")
        .option("enableModeCheckForSchemaFields", false)
        .save(testDataset + "." + TestConstants.DIFF_IN_SCHEMA_DEST_TABLE_NAME);
  }

  @Test
  public void testWriteToBigQueryPartitionedAndClusteredTable() {
    // partition write not supported in BQ Storage Write API
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.INDIRECT));

    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", TestConstants.LIBRARIES_PROJECTS_TABLE)
            .load()
            .where("platform = 'Sublime'");

    df.write()
        .format("bigquery")
        .option("table", fullTableNamePartitioned())
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("partitionField", "created_timestamp")
        .option("clusteredFields", "platform")
        .option("writeMethod", writeMethod.toString())
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
    Schema schema =
        Schema.of(
            Field.of("the_date", LegacySQLTypeName.DATE),
            Field.of("some_text", LegacySQLTypeName.STRING));
    TimePartitioning timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField("the_date").build();
    StandardTableDefinition tableDefinition =
        StandardTableDefinition.newBuilder()
            .setSchema(schema)
            .setTimePartitioning(timePartitioning)
            .build();
    bq.create(TableInfo.of(tableId, tableDefinition));
    // entering the data
    try {
      bq.query(
          QueryJobConfiguration.of(
              String.format(
                  "insert into `"
                      + fullTableName()
                      + "` (the_date, some_text) values ('2020-07-01', 'foo'), ('2020-07-02',"
                      + " 'bar')")));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // overrding a single partition
    List<Row> rows = Arrays.asList(RowFactory.create(Date.valueOf("2020-07-01"), "baz"));
    StructType newDataSchema =
        new StructType(
            new StructField[] {
              dateField, new StructField("some_text", DataTypes.StringType, true, Metadata.empty())
            });
    Dataset<Row> newDataDF = spark.createDataFrame(rows, newDataSchema);

    newDataDF
        .write()
        .format("bigquery")
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

  // TODO: figure out why the table creation fails
  // @Test
  public void testOverwriteSinglePartition() {
    overwriteSinglePartition(
        new StructField("the_date", DataTypes.DateType, true, Metadata.empty()));
  }

  // TODO: figure out why the table creation fails
  // @Test
  public void testOverwriteSinglePartitionWithComment() {
    String comment = "the partition field";
    Dataset<Row> resultDF =
        overwriteSinglePartition(
            new StructField("the_date", DataTypes.DateType, true, Metadata.empty())
                .withComment(comment));
    assertThat(resultDF.schema().fields()[0].getComment()).isEqualTo(Some.apply(comment));
  }

  @Test
  public void testWriteToBigQueryWithDescription() {
    String testDescription = "test description";
    String testComment = "test comment";

    Metadata metadata = Metadata.fromJson("{\"description\": \"" + testDescription + "\"}");

    StructType[] schemas =
        new StructType[] {
          structType(new StructField("c1", DataTypes.IntegerType, true, metadata)),
          structType(
              new StructField("c1", DataTypes.IntegerType, true, Metadata.empty())
                  .withComment(testComment)),
          structType(
              new StructField("c1", DataTypes.IntegerType, true, metadata)
                  .withComment(testComment)),
          structType(new StructField("c1", DataTypes.IntegerType, true, Metadata.empty()))
        };

    String[] readValues = new String[] {testDescription, testComment, testComment, null};

    for (int i = 0; i < schemas.length; i++) {
      List<Row> data = Arrays.asList(RowFactory.create(100), RowFactory.create(200));

      Dataset<Row> descriptionDF = spark.createDataFrame(data, schemas[i]);

      descriptionDF
          .write()
          .format("bigquery")
          .mode(SaveMode.Overwrite)
          .option("table", fullTableName() + "_" + i)
          .option("temporaryGcsBucket", temporaryGcsBucket)
          .option("intermediateFormat", "parquet")
          .option("writeMethod", writeMethod.toString())
          .save();

      Dataset<Row> readDF =
          spark
              .read()
              .format("bigquery")
              .option("dataset", testDataset.toString())
              .option("table", testTable + "_" + i)
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
    // partition write not supported in BQ Storage Write API
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.INDIRECT));

    List<Data> data =
        Arrays.asList(
            new Data("a", Timestamp.valueOf("2020-01-01 01:01:01")),
            new Data("b", Timestamp.valueOf("2020-01-02 02:02:02")),
            new Data("c", Timestamp.valueOf("2020-01-03 03:03:03")));
    Dataset<Row> df = spark.createDataset(data, Encoders.bean(Data.class)).toDF();
    String table = testDataset.toString() + "." + testTable + "_" + partitionType;
    df.write()
        .format("bigquery")
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("partitionField", "ts")
        .option("partitionType", partitionType)
        .option("partitionRequireFilter", "true")
        .option("table", table)
        .option("writeMethod", writeMethod.toString())
        .save();

    Dataset<Row> readDF = spark.read().format("bigquery").load(table);
    assertThat(readDF.count()).isEqualTo(3);
  }

  @Test
  public void testCacheDataFrameInDataSource() {
    // It takes some time for the data to be available for read via the Storage Read API, after it
    // has been written
    // using the Storage Write API hence this test becomes flaky!
    // Thus ignoring this for V2
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.INDIRECT));

    Dataset<Row> allTypesTable = readAllTypesTable();
    writeToBigQuery(allTypesTable, SaveMode.Overwrite, "avro");

    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset.toString())
            .option("table", testTable)
            .option("readDataFormat", "arrow")
            .load()
            .cache();

    assertThat(df.head()).isEqualTo(allTypesTable.head());

    // read from cache
    assertThat(df.head()).isEqualTo(allTypesTable.head());
    assertThat(df.schema()).isEqualTo(allTypesTable.schema());
  }

  protected long numberOfRowsWith(String name) {
    try {
      return bq.query(
              QueryJobConfiguration.of(
                  String.format("select name from %s where name='%s'", fullTableName(), name)))
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
