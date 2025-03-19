/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class Spark35ReadIntegrationTest extends ReadIntegrationTestBase {

  public Spark35ReadIntegrationTest() {

    super(/* userProvidedSchemaAllowed */ false, DataTypes.TimestampNTZType);
  }

  // tests are from the super-class
  @Test
  public void runParameterizedQuery() {
    // String Block not available
    String para_json = "{\n" +
                       "  \"corpus\": {\n" +
                       "    \"value\": \"romeoandjuliet\",\n" +
                       "    \"type\": \"STRING\"\n" +
                       "  },\n" +
                       "  \"min_word_count\": {\n" +
                       "    \"value\": \"250\",\n" +
                       "    \"type\": \"INT64\"\n" +
                       "  }\n" +
                       "}";
    Dataset<Row> parameterizedQueryDF = spark
        .read()
        .format("bigquery")
        .option("query", "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE corpus = @corpus AND word_count >= @min_word_count ORDER BY word_count DESC")
        .option("queryParameters", para_json)
        .option("viewsEnabled", "true")
        .load();

    parameterizedQueryDF.printSchema();
    parameterizedQueryDF.show();
    assertThat(parameterizedQueryDF.schema().fieldNames()).asList().containsExactly("word", "word_count");
    assertThat(parameterizedQueryDF.schema().fields()[0].dataType().typeName()).isEqualTo("string");
    assertThat(parameterizedQueryDF.schema().fields()[1].dataType().typeName()).isEqualTo("long");
    assertThat(parameterizedQueryDF.count()).isGreaterThan(0L);


  }
}
