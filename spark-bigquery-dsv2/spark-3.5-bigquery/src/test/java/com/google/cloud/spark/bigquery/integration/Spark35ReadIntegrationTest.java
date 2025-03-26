/*
 * Copyright 2021 Google LLC // Keep original copyright
 * Modified 2024 Google LLC // Add modification notice if appropriate
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
import static org.junit.Assert.assertThrows; // Assuming JUnit 4 style based on original

import com.google.inject.ProvisionException;
import org.apache.spark.sql.AnalysisException; // Import for potential Spark-level errors
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class Spark35ReadIntegrationTest extends ReadIntegrationTestBase {

  public Spark35ReadIntegrationTest() {
    // Keep constructor as is
    super(/* userProvidedSchemaAllowed */ false, DataTypes.TimestampNTZType);
  }
  // Query constants for clarity
  private static final String SHAKESPEARE_TABLE = "`bigquery-public-data.samples.shakespeare`";
  private static final String NAMED_PARAM_QUERY =
      "SELECT word, word_count FROM " + SHAKESPEARE_TABLE +
          " WHERE corpus = @corpus AND word_count >= @min_word_count ORDER BY word_count DESC";
  private static final String POSITIONAL_PARAM_QUERY =
      "SELECT word, word_count FROM " + SHAKESPEARE_TABLE +
          " WHERE corpus = ? AND word_count >= ? ORDER BY word_count DESC";
  private static final String NORMAL_QUERY =
      "SELECT word, word_count FROM " + SHAKESPEARE_TABLE +
          " WHERE corpus = 'romeoandjuliet' AND word_count >= 250 ORDER BY word_count DESC";


  @Test
  public void testReadWithNamedParameters() {
    Dataset<Row> df = spark
        .read()
        .format("bigquery")
        .option("query", NAMED_PARAM_QUERY)
        .option("viewsEnabled", "true") // Keep relevant options
        .option("NamedParameters.corpus", "STRING:romeoandjuliet") // New format
        .option("NamedParameters.min_word_count", "INT64:250") // New format
        .load();

    df.printSchema();
    df.show();
    assertThat(df.schema().fieldNames()).asList().containsExactly("word", "word_count");
    assertThat(df.schema().fields()[0].dataType().typeName()).isEqualTo("string");
    assertThat(df.schema().fields()[1].dataType().typeName()).isEqualTo("long");
    // Use Truth's assertion style
    assertThat(df.count()).isGreaterThan(0L);
  }

  @Test
  public void testReadWithPositionalParameters() {
    Dataset<Row> df = spark
        .read()
        .format("bigquery")
        .option("query", POSITIONAL_PARAM_QUERY) // Query uses '?'
        .option("viewsEnabled", "true")
        .option("PositionalParameters.1", "STRING:romeoandjuliet") // New format, index 1
        .option("PositionalParameters.2", "INT64:250") // New format, index 2
        .load();

    df.printSchema();
    df.show();
    assertThat(df.schema().fieldNames()).asList().containsExactly("word", "word_count");
    assertThat(df.schema().fields()[0].dataType().typeName()).isEqualTo("string");
    assertThat(df.schema().fields()[1].dataType().typeName()).isEqualTo("long");
    assertThat(df.count()).isGreaterThan(0L);
  }

  @Test
  public void testReadWithMixedParametersFails() {
    // 1. Expect the ProvisionException that is actually thrown
    ProvisionException thrown = assertThrows(ProvisionException.class,
        () -> {
          spark
              .read()
              .format("bigquery")
              .option("query", NAMED_PARAM_QUERY)
              .option("viewsEnabled", "true")
              .option("NamedParameters.corpus", "STRING:whatever")
              .option("PositionalParameters.1", "INT64:100")
              .load()
              .show();
        });

    Throwable cause = thrown.getCause();

    assertThat(cause).isNotNull();
    assertThat(cause).isInstanceOf(IllegalArgumentException.class);

    assertThat(cause)
        .hasMessageThat()
        .contains("Cannot mix NamedParameters.* and PositionalParameters.* options.");
  }


  @Test
  public void testReadWithNormalQuery() { // Renamed original test
    Dataset<Row> df = spark
        .read()
        .format("bigquery")
        .option("query", NORMAL_QUERY) // No parameters in query or options
        .option("viewsEnabled", "true")
        .load();

    df.printSchema();
    df.show();
    assertThat(df.schema().fieldNames()).asList().containsExactly("word", "word_count");
    assertThat(df.schema().fields()[0].dataType().typeName()).isEqualTo("string");
    assertThat(df.schema().fields()[1].dataType().typeName()).isEqualTo("long");
    assertThat(df.count()).isGreaterThan(0L);
  }

}