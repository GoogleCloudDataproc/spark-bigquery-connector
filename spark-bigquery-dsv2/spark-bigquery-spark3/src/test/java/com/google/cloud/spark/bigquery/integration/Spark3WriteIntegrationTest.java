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
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.spark.bigquery.integration.model.Data;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class Spark3WriteIntegrationTest extends WriteIntegrationTestBase {

  // tests are from the super-class
  @Override
  @Test
  public void testWriteToBigQuery_AppendSaveMode() {
    // initial write
    Dataset<Row> df = initialData();
    writeToBigQuery(df, SaveMode.Append);
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
    assertThrows(
        org.apache.spark.sql.AnalysisException.class,
        () -> {
          writeToBigQuery(additonalData(), SaveMode.ErrorIfExists);
        });
  }

  @Test
  public void testWriteToBigQuery_IgnoreSaveMode() {
    // initial write
    assertThrows(
        org.apache.spark.sql.AnalysisException.class,
        () -> {
          writeToBigQuery(additonalData(), SaveMode.Ignore);
        });
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
    assertThrows(
        org.apache.spark.sql.AnalysisException.class,
        () -> {
          writeToBigQuery(additonalData(), SaveMode.ErrorIfExists, "avro");
        });
  }

  @Test
  public void testWriteToBigQuerySimplifiedApi() {
    Dataset<Row> df = initialData();
    df.write()
        .format("bigquery")
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("schema", df.schema().toDDL())
        .mode(SaveMode.Append)
        .save(fullTableName());
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQueryAddingTheSettingsToSparkConf() {
    spark.conf().set("temporaryGcsBucket", temporaryGcsBucket);
    Dataset<Row> df = initialData();
    df.write()
        .format("bigquery")
        .option("table", fullTableName())
        .option("schema", df.schema().toDDL())
        .mode(SaveMode.Append)
        .save();
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQueryPartitionedAndClusteredTable() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("table", TestConstants.LIBRARIES_PROJECTS_TABLE)
            .option("filter", "platform = 'Sublime'")
            .load();

    df.write()
        .format("bigquery")
        .option("table", fullTableNamePartitioned())
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("partitionField", "created_timestamp")
        .option("clusteredFields", "platform")
        .option("schema", df.schema().toDDL())
        .mode(SaveMode.Overwrite)
        .save();

    StandardTableDefinition tableDefinition = super.testPartitionedTableDefinition();
    assertThat(tableDefinition.getTimePartitioning().getField()).isEqualTo("created_timestamp");
    assertThat(tableDefinition.getClustering().getFields()).contains("platform");
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
        .option("schema", df.schema().toDDL())
        .mode(SaveMode.Append)
        .save();

    Dataset<Row> readDF = spark.read().format("bigquery").load(table);
    assertThat(readDF.count()).isEqualTo(3);
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

      //      writeToBigQuery(descriptionDF, SaveMode.Overwrite);
      descriptionDF
          .write()
          .format("bigquery")
          .mode(SaveMode.Append)
          .option("table", fullTableName())
          .option("temporaryGcsBucket", temporaryGcsBucket)
          .option("intermediateFormat", "parquet")
          .option("schema", descriptionDF.schema().toDDL())
          .save();
      //      Dataset<Row> readDF =
      //              spark
      //                      .read()
      //                      .format("bigquery")
      //                      .option("dataset", testDataset.toString())
      //                      .option("table", testTable)
      //                      .load();
      //
      //      Optional<String> description =
      //              SchemaConverters.getDescriptionOrCommentOfField(readDF.schema().fields()[0]);
      //
      //      if (readValues[i] != null) {
      //        assertThat(description.isPresent()).isTrue();
      //        assertThat(description.orElse("")).isEqualTo(readValues[i]);
      //      } else {
      //        assertThat(description.isPresent()).isFalse();
      //      }
    }
  }

  @Override
  @Test
  public void testCacheDataFrameInDataSource() {
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
}
