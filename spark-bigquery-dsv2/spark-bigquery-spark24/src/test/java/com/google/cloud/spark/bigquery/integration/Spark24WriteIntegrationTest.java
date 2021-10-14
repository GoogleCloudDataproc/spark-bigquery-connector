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

import com.google.cloud.RetryOption;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.Arrays;

import static com.google.cloud.spark.bigquery.integration.TestConstants.STORAGE_API_ALL_TYPES_ROWS;
import static com.google.cloud.spark.bigquery.integration.TestConstants.STORAGE_API_ALL_TYPES_SCHEMA;
import static com.google.cloud.spark.bigquery.integration.TestConstants.STORAGE_API_ALL_TYPES_SCHEMA_BIGQUERY_REPRESENTATION;
import static com.google.common.truth.Truth.assertThat;
import static java.lang.String.format;
import static org.junit.Assert.fail;

public class Spark24WriteIntegrationTest extends WriteIntegrationTestBase {

  public Spark24WriteIntegrationTest() {
    super(true);
  }

  // Numeric is a fixed precision Decimal Type with 38 digits of precision and 9 digits of scale.
  // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
  public static final Logger logger = LogManager.getLogger("com.google.cloud");

  public static final String PROJECT = ServiceOptions.getDefaultProjectId();
  public static final String DATASET = "spark_bigquery_storage_write_it_" + System.nanoTime();
  public static final String OVERWRITE_TABLE = "testTable";
  public static final String DESCRIPTION = "Spark BigQuery connector write session tests.";

  public static final String BIGQUERY_PUBLIC_DATA = "bigquery-public-data";
  public static final String SMALL_DATA_DATASET = "san_francisco_bikeshare";
  public static final String SMALL_DATA_TABLE = "bikeshare_station_status";
  public static final String SMALL_DATA_ID =
      BIGQUERY_PUBLIC_DATA + ":" + SMALL_DATA_DATASET + "." + SMALL_DATA_TABLE;
  public static final String MB20_DATASET = "baseball";
  public static final String MB20_TABLE = "games_post_wide";
  public static final String MB20_ID = BIGQUERY_PUBLIC_DATA + ":" + MB20_DATASET + "." + MB20_TABLE;
  public static final String MB100_DATASET = "open_images";
  public static final String MB100_TABLE = "annotations_bbox";
  public static final String MB100_ID =
      BIGQUERY_PUBLIC_DATA + ":" + MB100_DATASET + "." + MB100_TABLE; // 156 MB

  public static SparkSession spark;
  public static BigQuery bigquery;

  public static Dataset<Row> allTypesDf;
  public static Dataset<Row> twiceAsBigDf;
  public static Dataset<Row> smallDataDf;
  public static Dataset<Row> MB20Df;
  public static Dataset<Row> MB100Df;

  @BeforeClass
  public static void init() throws Exception {
    logger.setLevel(Level.DEBUG);
    spark =
        SparkSession.builder()
            .appName("Application Name")
            .config("some-config", "some-value")
            .master("local[*]")
            .getOrCreate();
    allTypesDf =
        spark.createDataFrame(
            Arrays.asList(STORAGE_API_ALL_TYPES_ROWS), STORAGE_API_ALL_TYPES_SCHEMA);
    smallDataDf = spark.read().format("bigquery").option("table", SMALL_DATA_ID).load();
    twiceAsBigDf = smallDataDf.unionAll(smallDataDf);
    MB20Df =
        spark
            .read()
            .format("bigquery")
            .option("table", MB20_ID)
            .load()
            .drop("startTime")
            .drop("createdAt")
            .drop("updatedAt") /*.coalesce(20)*/
            .toDF();
    MB100Df =
        spark.read().format("bigquery").option("table", MB100_ID).load() /*.coalesce(20).toDF()*/;

    bigquery = BigQueryOptions.getDefaultInstance().getService();
    DatasetInfo datasetInfo =
        DatasetInfo.newBuilder(/* datasetId = */ DATASET).setDescription(DESCRIPTION).build();
    bigquery.create(datasetInfo);
    logger.info("Created test dataset: " + DATASET);

    // create small data frame inside of our test data-set (to be over-written by the twice-as-big
    // dataframe in
    // testSparkOverWriteSaveMode()
    TableId overWriteTableId = TableId.of(PROJECT, DATASET, OVERWRITE_TABLE);
    TableId smallDataTableId =
        TableId.of(BIGQUERY_PUBLIC_DATA, SMALL_DATA_DATASET, SMALL_DATA_TABLE);
    bigquery.create(
        TableInfo.of(overWriteTableId, bigquery.getTable(smallDataTableId).getDefinition()));
    copyTable(smallDataTableId, overWriteTableId);
  }

  public static void copyTable(TableId from, TableId to) {
    String queryFormat = "INSERT INTO `%s`\n" + "SELECT * FROM `%s`";
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(sqlFromFormat(queryFormat, to, from))
            .setUseLegacySql(false)
            .build();

    Job copy = bigquery.create(JobInfo.newBuilder(queryConfig).build());

    try {
      Job completedJob =
          copy.waitFor(
              RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
              RetryOption.totalTimeout(Duration.ofMinutes(3)));
      if (completedJob == null && completedJob.getStatus().getError() != null) {
        throw new IOException(completedJob.getStatus().getError().toString());
      }
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(
          "Could not copy table from temporary sink to destination table.", e);
    }
  }

  static String sqlFromFormat(
      String queryFormat, TableId destinationTableId, TableId temporaryTableId) {
    return String.format(
        queryFormat, fullTableName(destinationTableId), fullTableName(temporaryTableId));
  }

  static String fullTableName(TableId tableId) {
    return format("%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }

  @AfterClass
  public static void close() throws Exception {
    if (bigquery != null) {
      bigquery.delete(DATASET, BigQuery.DatasetDeleteOption.deleteContents());
      logger.info("Deleted test dataset: " + DATASET);
    }
  }

  @Test
  public void testSparkBigQueryWriteAllTypes() throws Exception {
    String writeTo = "all_types";

    Dataset<Row> expectedDF = allTypesDf;

    expectedDF
        .write()
        .format("bigquery")
        .option("table", writeTo)
        .option("writePath", "direct")
        .option("dataset", DATASET)
        .option("project", PROJECT)
        .mode(SaveMode.Overwrite)
        .save();

    Dataset<Row> actualDF =
        spark
            .read()
            .format("bigquery")
            .option("table", writeTo)
            .option("dataset", DATASET)
            .option("project", PROJECT)
            .load();

    assertThat(actualDF.schema()).isEqualTo(STORAGE_API_ALL_TYPES_SCHEMA_BIGQUERY_REPRESENTATION);

    Dataset<Row> intersection = actualDF.intersectAll(expectedDF);
    assertThat(
        intersection.count() == actualDF.count() && intersection.count() == expectedDF.count());
  }

  @Test
  public void testSparkAppendSaveMode() throws Exception {
    String writeTo = "append";

    Dataset<Row> expectedDF = twiceAsBigDf;

    smallDataDf
        .write()
        .format("bigquery")
        .option("table", writeTo)
        .option("dataset", DATASET)
        .option("project", PROJECT)
        .save();

    smallDataDf
        .write()
        .format("bigquery")
        .option("table", writeTo)
        .option("dataset", DATASET)
        .option("project", PROJECT)
        .mode(SaveMode.Append)
        .save();

    Dataset<Row> actualDF =
        spark
            .read()
            .format("bigquery")
            .option("table", writeTo)
            .option("dataset", DATASET)
            .option("project", PROJECT)
            .load();

    Dataset<Row> intersection = actualDF.intersectAll(expectedDF);
    // append was successful:
    assertThat(
        intersection.count() == actualDF.count() && intersection.count() == expectedDF.count());
  }

  @Test
  public void testSparkOverWriteSaveMode() throws Exception {
    String writeTo = OVERWRITE_TABLE;

    Dataset<Row> expectedDF = twiceAsBigDf;

    twiceAsBigDf
        .write()
        .format("bigquery")
        .option("table", writeTo)
        .option("dataset", DATASET)
        .option("project", PROJECT)
        .mode(SaveMode.Overwrite)
        .save();

    Dataset<Row> actualDF =
        spark
            .read()
            .format("bigquery")
            .option("table", writeTo)
            .option("dataset", DATASET)
            .option("project", PROJECT)
            .load();

    Dataset<Row> intersection = actualDF.intersectAll(expectedDF);
    // overwrite was successful:
    assertThat(
        intersection.count() == actualDF.count() && intersection.count() == expectedDF.count());
  }

  @Test
  public void testSparkWriteIgnoreSaveMode() throws Exception {
    String writeTo = "ignore";

    Dataset<Row> expectedDF = smallDataDf;

    smallDataDf
        .write()
        .format("bigquery")
        .option("table", writeTo)
        .option("dataset", DATASET)
        .option("project", PROJECT)
        .save();

    twiceAsBigDf
        .write()
        .format("bigquery")
        .option("table", writeTo)
        .option("dataset", DATASET)
        .option("project", PROJECT)
        .mode(SaveMode.Ignore)
        .save();

    Dataset<Row> actualDF =
        spark
            .read()
            .format("bigquery")
            .option("table", writeTo)
            .option("dataset", DATASET)
            .option("project", PROJECT)
            .load();

    Dataset<Row> intersection = actualDF.intersectAll(expectedDF);
    // ignore was successful:
    assertThat(
        intersection.count() == actualDF.count() && intersection.count() == expectedDF.count());
  }

  @Test
  public void testSparkWriteErrorSaveMode() throws Exception {
    String writeTo = "error";

    smallDataDf
        .write()
        .format("bigquery")
        .option("table", writeTo)
        .option("dataset", DATASET)
        .option("project", PROJECT)
        .save();

    try {
      smallDataDf
          .write()
          .format("bigquery")
          .option("table", writeTo)
          .option("dataset", DATASET)
          .option("project", PROJECT)
          .mode(SaveMode.ErrorIfExists)
          .save();
      fail("Did not throw an error for ErrorIfExists");
    } catch (RuntimeException e) {
      // Successfully threw an exception for ErrorIfExists
    }
  }

  @Test
  public void testSparkBigQueryWrite20MB() throws Exception {
    String writeTo = "20MB";

    MB20Df.write()
        .format("bigquery")
        .option("table", writeTo)
        .option("dataset", DATASET)
        .option("project", PROJECT)
        .mode(SaveMode.Overwrite)
        .save();

    // TODO: simple num bytes print line

    assertThat(
        bigquery
            .getTable(TableId.of(PROJECT, DATASET, writeTo))
            .getNumBytes()
            .equals(
                bigquery
                    .getTable(TableId.of(BIGQUERY_PUBLIC_DATA, MB20_DATASET, MB20_TABLE))
                    .getNumBytes()));
  }

  @Test
  public void testSparkBigQueryWrite100MB() throws Exception {
    String writeTo = "100MB";

    MB100Df.write()
        .format("bigquery")
        .option("table", writeTo)
        .option("dataset", DATASET)
        .option("project", PROJECT)
        .mode(SaveMode.Overwrite)
        .save();

    assertThat(
        bigquery
            .getTable(TableId.of(PROJECT, DATASET, writeTo))
            .getNumBytes()
            .equals(
                bigquery
                    .getTable(TableId.of(BIGQUERY_PUBLIC_DATA, MB100_DATASET, MB100_TABLE))
                    .getNumBytes()));
  }
}
