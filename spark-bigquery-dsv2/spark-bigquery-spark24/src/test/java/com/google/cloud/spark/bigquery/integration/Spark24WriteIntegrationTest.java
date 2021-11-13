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

import com.google.cloud.bigquery.StandardTableDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Test;

public class Spark24WriteIntegrationTest extends WriteIntegrationTestBase {

  // tests are from the super-class
  @Override
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
    initialData()
        .write()
        .format("bigquery")
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .save(fullTableName());
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQueryAddingTheSettingsToSparkConf() {
    spark.conf().set("temporaryGcsBucket", temporaryGcsBucket);
    initialData().write().format("bigquery").option("table", fullTableName()).save();
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
            .load()
            .where("platform = 'Sublime'");

    df.write()
        .format("bigquery")
        .option("table", fullTableNamePartitioned())
        .option("temporaryGcsBucket", temporaryGcsBucket)
        .option("partitionField", "created_timestamp")
        .option("clusteredFields", "platform")
        .mode(SaveMode.Overwrite)
        .save();

    StandardTableDefinition tableDefinition = super.testPartitionedTableDefinition();
    assertThat(tableDefinition.getTimePartitioning().getField()).isEqualTo("created_timestamp");
    assertThat(tableDefinition.getClustering().getFields()).contains("platform");
  }
}
