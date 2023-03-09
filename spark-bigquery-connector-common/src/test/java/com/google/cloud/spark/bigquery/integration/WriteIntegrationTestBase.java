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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeThat;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig.WriteMethod;
import com.google.cloud.spark.bigquery.integration.model.Data;
import com.google.cloud.spark.bigquery.integration.model.Friend;
import com.google.cloud.spark.bigquery.integration.model.Link;
import com.google.cloud.spark.bigquery.integration.model.Person;
import com.google.common.collect.ImmutableMap;
import com.google.inject.ProvisionException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
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
import org.junit.Test;
import scala.Some;

abstract class WriteIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

  protected static AtomicInteger id = new AtomicInteger(0);
  protected final SparkBigQueryConfig.WriteMethod writeMethod;
  protected Class<? extends Exception> expectedExceptionOnExistingTable;
  protected BigQuery bq;

  public WriteIntegrationTestBase(SparkBigQueryConfig.WriteMethod writeMethod) {
    this(writeMethod, IllegalArgumentException.class);
  }

  public WriteIntegrationTestBase(
      SparkBigQueryConfig.WriteMethod writeMethod,
      Class<? extends Exception> expectedExceptionOnExistingTable) {
    super();
    this.writeMethod = writeMethod;
    this.expectedExceptionOnExistingTable = expectedExceptionOnExistingTable;
    this.bq = BigQueryOptions.getDefaultInstance().getService();
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

  private String createDiffInSchemaDestTable() {
    String destTableName = TestConstants.DIFF_IN_SCHEMA_DEST_TABLE_NAME + "_" + System.nanoTime();
    IntegrationTestUtils.runQuery(
        String.format(TestConstants.DIFF_IN_SCHEMA_DEST_TABLE, testDataset, destTableName));
    return destTableName;
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
    writeToBigQuery(df, mode, "avro");
  }

  protected void writeToBigQuery(Dataset<Row> df, SaveMode mode, String format) {
    df.write()
        .format("bigquery")
        .mode(mode)
        .option("table", fullTableName())
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
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
  public void testWriteToBigQuery_WithTableLabels() {
    Dataset<Row> df = initialData();

    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("table", fullTableName())
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("intermediateFormat", "avro")
        .option("writeMethod", writeMethod.toString())
        .option("bigQueryTableLabel.alice", "bob")
        .option("bigQueryTableLabel.foo", "bar")
        .save();

    Table bigQueryTable = bq.getTable(testDataset.toString(), testTable);
    Map<String, String> tableLabels = bigQueryTable.getLabels();
    assertEquals(2, tableLabels.size());
    assertEquals("bob", tableLabels.get("alice"));
    assertEquals("bar", tableLabels.get("foo"));
  }

  @Test
  public void testWriteToBigQuery_EnableListInference() throws InterruptedException {
    Dataset<Row> df = initialData();
    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("table", fullTableName())
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("intermediateFormat", "parquet")
        .option("writeMethod", writeMethod.toString())
        .option("enableListInference", true)
        .save();

    Dataset<Row> readDF =
        spark
            .read()
            .format("bigquery")
            .option("dataset", testDataset.toString())
            .option("table", testTable)
            .load();
    Schema initialSchema = SchemaConverters.toBigQuerySchema(df.schema());
    Schema readSchema = SchemaConverters.toBigQuerySchema(readDF.schema());
    assertEquals(initialSchema, readSchema);
  }

  @Test
  public void testWriteToBigQuery_ErrorIfExistsSaveMode() throws InterruptedException {
    // initial write
    writeToBigQuery(initialData(), SaveMode.ErrorIfExists);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    assertThrows(
        expectedExceptionOnExistingTable,
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
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("writeMethod", writeMethod.toString())
        .save(fullTableName());
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQueryAddingTheSettingsToSparkConf() throws InterruptedException {
    spark.conf().set("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET);
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
  public void testDirectWriteToBigQueryWithDiffInSchema() {
    assumeThat(writeMethod, equalTo(WriteMethod.DIRECT));
    String destTableName = createDiffInSchemaDestTable();
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
                .option("writeMethod", writeMethod.toString())
                .save(testDataset + "." + destTableName));
  }

  @Test
  public void testDirectWriteToBigQueryWithDiffInSchemaAndDisableModeCheck() throws Exception {
    assumeThat(writeMethod, equalTo(WriteMethod.DIRECT));
    String destTableName = createDiffInSchemaDestTable();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", testDataset + "." + TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_NAME)
            .load();

    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("writeMethod", writeMethod.toString())
        .option("enableModeCheckForSchemaFields", false)
        .save(testDataset + "." + destTableName);
    String query = String.format("select * from %s.%s", testDataset.toString(), destTableName);
    int numOfRows = (int) bq.query(QueryJobConfiguration.of(query)).getTotalRows();
    assertThat(numOfRows).isEqualTo(1);
  }

  @Test
  public void testDirectWriteToBigQueryWithDiffInDescription() throws Exception {
    assumeThat(writeMethod, equalTo(WriteMethod.DIRECT));
    String destTableName = createDiffInSchemaDestTable();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option(
                "table",
                testDataset + "." + TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_NAME_WITH_DESCRIPTION)
            .load();

    assertThrows(
        ProvisionException.class,
        () ->
            df.write()
                .format("bigquery")
                .mode(SaveMode.Append)
                .option("writeMethod", writeMethod.toString())
                .save(testDataset + "." + destTableName));
  }

  @Test
  public void testInDirectWriteToBigQueryWithDiffInSchemaAndModeCheck() throws Exception {
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.INDIRECT));
    String destTableName = createDiffInSchemaDestTable();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", testDataset + "." + TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_NAME)
            .load();

    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("writeMethod", writeMethod.toString())
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("enableModeCheckForSchemaFields", true)
        .save(testDataset + "." + destTableName);
    String query = String.format("select * from %s.%s", testDataset.toString(), destTableName);
    int numOfRows = (int) bq.query(QueryJobConfiguration.of(query)).getTotalRows();
    assertThat(numOfRows).isEqualTo(1);
  }

  @Test
  public void testIndirectWriteToBigQueryWithDiffInSchemaNullableFieldAndDisableModeCheck()
      throws Exception {
    assumeThat(writeMethod, equalTo(SparkBigQueryConfig.WriteMethod.INDIRECT));
    String destTableName = createDiffInSchemaDestTable();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", testDataset + "." + TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_NAME)
            .load();

    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("writeMethod", writeMethod.toString())
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("enableModeCheckForSchemaFields", false)
        .save(testDataset + "." + destTableName);
    String query = String.format("select * from %s.%s", testDataset.toString(), destTableName);
    int numOfRows = (int) bq.query(QueryJobConfiguration.of(query)).getTotalRows();
    assertThat(numOfRows).isEqualTo(1);
  }

  @Test
  public void testInDirectWriteToBigQueryWithDiffInDescription() throws Exception {
    assumeThat(writeMethod, equalTo(WriteMethod.INDIRECT));
    String destTableName = createDiffInSchemaDestTable();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option(
                "table",
                testDataset + "." + TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_NAME_WITH_DESCRIPTION)
            .load();

    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("writeMethod", writeMethod.toString())
        .save(testDataset + "." + destTableName);
    String query = String.format("select * from %s.%s", testDataset.toString(), destTableName);
    int numOfRows = (int) bq.query(QueryJobConfiguration.of(query)).getTotalRows();
    assertThat(numOfRows).isEqualTo(1);
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
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
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
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
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
          .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
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

  @Test
  public void testWriteEmptyDataFrame() throws Exception {
    Dataset<Row> df = spark.createDataFrame(Collections.emptyList(), Link.class);
    writeToBigQuery(df, SaveMode.Append);
    assertThat(testTableNumberOfRows()).isEqualTo(0);
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
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
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

  @Test
  public void testWriteJsonToANewTable() throws Exception {
    assumeThat(writeMethod, equalTo(WriteMethod.INDIRECT));
    Dataset<Row> df =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create("{\"key\":\"foo\",\"value\":1}"),
                RowFactory.create("{\"key\":\"bar\",\"value\":2}")),
            structType(
                StructField.apply(
                    "jf",
                    DataTypes.StringType,
                    true,
                    Metadata.fromJson("{\"sqlType\":\"JSON\"}"))));
    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("intermediateFormat", "AVRO")
        .option("dataset", testDataset.toString())
        .option("table", testTable)
        .option("writeMethod", writeMethod.toString())
        .save();

    Table table = bq.getTable(TableId.of(testDataset.toString(), testTable));
    assertThat(table).isNotNull();
    Schema schema = table.getDefinition().getSchema();
    assertThat(schema).isNotNull();
    assertThat(schema.getFields()).hasSize(1);
    assertThat(schema.getFields().get(0).getType()).isEqualTo(LegacySQLTypeName.JSON);

    // validating by querying a sub-field of the json
    Dataset<Row> resultDF =
        spark
            .read()
            .format("bigquery")
            .option("viewsEnabled", "true")
            .option("materializationDataset", testDataset.toString())
            .load(String.format("SELECT jf.value FROM `%s.%s`", testDataset.toString(), testTable));
    // collecting the data to validate its content
    List<String> result =
        resultDF.collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    assertThat(result).containsExactly("1", "2");
  }

  @Test
  public void testWriteJsonToAnExistingTable() throws Exception {
    assumeThat(writeMethod, equalTo(WriteMethod.INDIRECT));
    Table table =
        bq.create(
            TableInfo.of(
                TableId.of(testDataset.toString(), testTable),
                StandardTableDefinition.of(Schema.of(Field.of("jf", LegacySQLTypeName.JSON)))));
    assertThat(table).isNotNull();
    Dataset<Row> df =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create("{\"key\":\"foo\",\"value\":1}"),
                RowFactory.create("{\"key\":\"bar\",\"value\":2}")),
            structType(StructField.apply("jf", DataTypes.StringType, true, Metadata.empty())));
    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("intermediateFormat", "AVRO")
        .option("dataset", testDataset.toString())
        .option("table", testTable)
        .option("writeMethod", writeMethod.toString())
        .save();

    // validating by querying a sub-field of the json
    Dataset<Row> resultDF =
        spark
            .read()
            .format("bigquery")
            .option("viewsEnabled", "true")
            .option("materializationDataset", testDataset.toString())
            .load(String.format("SELECT jf.value FROM `%s.%s`", testDataset.toString(), testTable));
    // collecting the data to validate its content
    List<String> result =
        resultDF.collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    assertThat(result).containsExactly("1", "2");
  }

  @Test
  public void testWriteMapToANewTable() throws Exception {
    Dataset<Row> df =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(ImmutableMap.of("a", new Long(1), "b", new Long(2))),
                RowFactory.create(ImmutableMap.of("c", new Long(3)))),
            structType(
                StructField.apply(
                    "mf",
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType),
                    false,
                    Metadata.empty())));
    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("intermediateFormat", "AVRO")
        .option("dataset", testDataset.toString())
        .option("table", testTable)
        .option("writeMethod", writeMethod.toString())
        .save();

    Table table = bq.getTable(TableId.of(testDataset.toString(), testTable));
    assertThat(table).isNotNull();
    Schema schema = table.getDefinition().getSchema();
    assertThat(schema).isNotNull();
    assertThat(schema.getFields()).hasSize(1);
    Field mapField = schema.getFields().get(0);
    assertThat(mapField.getType()).isEqualTo(LegacySQLTypeName.RECORD);
    assertThat(mapField.getMode()).isEqualTo(Mode.REPEATED);
    assertThat(mapField.getSubFields())
        .containsExactlyElementsIn(
            Arrays.asList(
                Field.newBuilder("key", LegacySQLTypeName.STRING).setMode(Mode.REQUIRED).build(),
                Field.newBuilder("value", LegacySQLTypeName.INTEGER)
                    .setMode(Mode.NULLABLE)
                    .build()));

    String sql =
        ("SELECT\n"
                + "  (SELECT COUNT(f.key) FROM TABLE, UNNEST(mf) AS f) AS total_keys,\n"
                + "  (SELECT COUNT(*) FROM TABLE) AS total_rows,\n"
                + "  (SELECT f.value FROM TABLE, UNNEST(mf) AS f WHERE f.key='b') AS b_value;")
            .replaceAll("TABLE", testDataset.toString() + "." + testTable);

    // validating by querying a sub-field of the json
    Dataset<Row> resultDF =
        spark
            .read()
            .format("bigquery")
            .option("viewsEnabled", "true")
            .option("materializationDataset", testDataset.toString())
            .load(sql);
    // collecting the data to validate its content
    Row result = resultDF.head();
    assertThat(result.getLong(0)).isEqualTo(3L);
    assertThat(result.getLong(1)).isEqualTo(2L);
    assertThat(result.getLong(2)).isEqualTo(2L);
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
