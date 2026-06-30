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

import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.spark.bigquery.events.BigQueryJobCompletedEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.ProvisionException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

class ReadFromQueryIntegrationTestBase extends SparkBigQueryIntegrationTestBaseV2 {

  protected SparkBigQueryIntegrationTestRunner testRunner =
      new InMemorySparkBigQueryIntegrationTestRunner();

  private final boolean isDsv2OnSpark3AndAbove;

  protected ReadFromQueryIntegrationTestBase() {
    this(false);
  }

  protected ReadFromQueryIntegrationTestBase(boolean isDsv2OnSpark3AndAbove) {
    super();
    this.isDsv2OnSpark3AndAbove = isDsv2OnSpark3AndAbove;
  }

  // =========================================================================
  // SCENARIO: SQL Queries executions (Cluster App)
  // =========================================================================

  @SuppressWarnings("resource")
  protected static JsonObject readQueryApp(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception {

    String scenario = parameters.getOrDefault("scenario", "STANDARD");
    boolean isDsv2OnSpark3AndAbove =
        Boolean.parseBoolean(parameters.getOrDefault("isDsv2", "false"));

    SparkSession spark =
        IntegrationTestUtils.createSparkSessionBuilder("ReadQueryTestApp")
            .getOrCreate()
            .newSession();
    TestBigQueryJobCompletionListener listener = new TestBigQueryJobCompletionListener();
    spark.sparkContext().addSparkListener(listener);

    try {
      JsonObject result = new JsonObject();
      result.addProperty("status", "success");

      String random = String.valueOf(System.nanoTime());
      String query =
          String.format(
              "SELECT corpus, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE word='spark' AND '%s'='%s'",
              random, random);

      Dataset<Row> df = null;
      if ("NO_MATERIALIZATION_DATASET".equals(scenario)) {
        df = spark.read().format("bigquery").option("viewsEnabled", true).load(query);
        result.addProperty("count", df.count());

      } else if ("STANDARD".equals(scenario)) {
        df =
            spark
                .read()
                .format("bigquery")
                .option("viewsEnabled", true)
                .option("materializationDataset", testDataset)
                .load(query);
        result.addProperty("count", df.count());

      } else if ("NEW_LINE".equals(scenario)) {
        String qNewLine =
            String.format(
                "SELECT corpus, word_count FROM `bigquery-public-data.samples.shakespeare`\nWHERE word='spark' AND '%s'='%s'",
                random, random);
        df =
            spark
                .read()
                .format("bigquery")
                .option("viewsEnabled", true)
                .option("materializationDataset", testDataset)
                .load(qNewLine);
        result.addProperty("count", df.count());

      } else if ("QUERY_OPTION".equals(scenario)) {
        df =
            spark
                .read()
                .format("bigquery")
                .option("viewsEnabled", true)
                .option("materializationDataset", testDataset)
                .option("query", query)
                .load();

        StructType expectedSchema =
            DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("corpus", DataTypes.StringType, true),
                    DataTypes.createStructField("word_count", DataTypes.LongType, true)));

        boolean noBqcTables = true;
        if (isDsv2OnSpark3AndAbove) {
          Iterable<Table> tablesInDataset =
              IntegrationTestUtils.listTables(
                  DatasetId.of(testDataset),
                  TableDefinition.Type.TABLE,
                  TableDefinition.Type.MATERIALIZED_VIEW);
          noBqcTables =
              StreamSupport.stream(tablesInDataset.spliterator(), false)
                  .noneMatch(table -> table.getTableId().getTable().startsWith("_bqc_"));
        }

        result.addProperty("count", df.count());
        result.addProperty("schemaMatches", df.schema().equals(expectedSchema));
        result.addProperty("noBqcTables", noBqcTables);

      } else if ("AUTO_GENERATED_TABLE".equals(scenario)) {
        df =
            spark
                .read()
                .format("bigquery")
                .option("viewsEnabled", true)
                .option("query", query)
                .load();
        StructType expectedSchema =
            DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("corpus", DataTypes.StringType, true),
                    DataTypes.createStructField("word_count", DataTypes.LongType, true)));

        result.addProperty("count", df.count());
        result.addProperty("schemaMatches", df.schema().equals(expectedSchema));

      } else if ("BAD_QUERY".equals(scenario)) {
        String badSql = "SELECT bogus_column FROM `bigquery-public-data.samples.shakespeare`";
        spark
            .read()
            .format("bigquery")
            .option("viewsEnabled", true)
            .option("materializationDataset", testDataset)
            .load(badSql);

      } else if ("PRIORITY".equals(scenario)) {
        df =
            spark
                .read()
                .format("bigquery")
                .option("viewsEnabled", true)
                .option("materializationDataset", testDataset)
                .option("queryJobPriority", "batch")
                .load(query);
        result.addProperty("count", df.count());

      } else if ("TIMEOUT".equals(scenario)) {
        String qLong = "SELECT * FROM `largesamples.wikipedia_pageviews_201001`";
        spark
            .read()
            .format("bigquery")
            .option("viewsEnabled", true)
            .option("materializationDataset", testDataset)
            .option("bigQueryJobTimeoutInMinutes", "1")
            .load(qLong)
            .show();

      } else if ("NAMED_PARAMETERS".equals(scenario)) {
        String namedParamQuery =
            "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE corpus = @corpus AND word_count >= @min_word_count ORDER BY word_count DESC";
        df =
            spark
                .read()
                .format("bigquery")
                .option("query", namedParamQuery)
                .option("viewsEnabled", "true")
                .option("materializationDataset", testDataset)
                .option("NamedParameters.corpus", "STRING:romeoandjuliet")
                .option("NamedParameters.min_word_count", "INT64:250")
                .load();

        StructType expectedSchema =
            DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("word", DataTypes.StringType, true),
                    DataTypes.createStructField("word_count", DataTypes.LongType, true)));

        JsonArray schemaColumns =
            new Gson()
                .toJsonTree(
                    Arrays.asList(df.schema().fields()).stream().map(StructField::name).toArray())
                .getAsJsonArray();
        result.addProperty("count", df.count());
        result.addProperty("schemaMatches", df.schema().equals(expectedSchema));
        result.add("schemaColumns", schemaColumns);

      } else if ("POSITIONAL_PARAMETERS".equals(scenario)) {
        String positionalParamQuery =
            "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE corpus = ? AND word_count >= ? ORDER BY word_count DESC";
        df =
            spark
                .read()
                .format("bigquery")
                .option("query", positionalParamQuery)
                .option("viewsEnabled", "true")
                .option("materializationDataset", testDataset)
                .option("PositionalParameters.1", "STRING:romeoandjuliet")
                .option("PositionalParameters.2", "INT64:250")
                .load();

        StructType expectedSchema =
            DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("word", DataTypes.StringType, true),
                    DataTypes.createStructField("word_count", DataTypes.LongType, true)));

        result.addProperty("count", df.count());
        result.addProperty("schemaMatches", df.schema().equals(expectedSchema));

      } else if ("MIXED_PARAMETERS_FAILS".equals(scenario)) {
        String queryForFailure =
            "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE corpus = @corpus AND word_count >= @min_word_count ORDER BY word_count DESC";
        spark
            .read()
            .format("bigquery")
            .option("query", queryForFailure)
            .option("viewsEnabled", "true")
            .option("materializationDataset", testDataset)
            .option("NamedParameters.corpus", "STRING:whatever")
            .option("PositionalParameters.1", "INT64:100")
            .load()
            .show();

      } else if ("KMS_KEY".equals(scenario)) {
        String envKmsKey = System.getenv("BIGQUERY_KMS_KEY_NAME");
        String kmsKeyName =
            envKmsKey != null ? envKmsKey : "projects/p/locations/l/keyRings/k/cryptoKeys/c";

        spark
            .read()
            .format("bigquery")
            .option("viewsEnabled", true)
            .option("materializationDataset", testDataset)
            .option("destinationTableKmsKeyName", kmsKeyName)
            .load(query)
            .collect();
      }

      if (df != null) {
        List<Row> rows = df.select(df.columns()[0]).distinct().collectAsList();
        JsonArray contentArray = new JsonArray();
        for (Row r : rows) {
          if (r.get(0) != null) {
            contentArray.add(r.get(0).toString());
          }
        }
        result.add("contentList", contentArray);
      }

      JsonArray jobsArray = new JsonArray();
      for (JobInfo job : listener.getJobInfos()) {
        JsonObject jobJson = new JsonObject();
        jobJson.addProperty("type", job.getConfiguration().getType().toString());
        if (job.getConfiguration() instanceof QueryJobConfiguration) {
          QueryJobConfiguration qConfig = (QueryJobConfiguration) job.getConfiguration();
          jobJson.addProperty("query", qConfig.getQuery());
          if (qConfig.getDestinationEncryptionConfiguration() != null) {
            jobJson.addProperty(
                "kmsKeyName", qConfig.getDestinationEncryptionConfiguration().getKmsKeyName());
          }
        }
        jobsArray.add(jobJson);
      }
      result.add("jobInfos", jobsArray);

      return result;
    } finally {
      spark.sparkContext().removeSparkListener(listener);
    }
  }

  private static void validateResult(JsonObject result) {
    long totalRows = result.get("count").getAsLong();
    assertThat(totalRows).isEqualTo(9);
    if (result.has("contentList")) {
      JsonArray contentList = result.getAsJsonArray("contentList");
      assertThat(contentList.size()).isGreaterThan(0);
      assertThat(contentList)
          .containsExactlyElementsIn(
              new Gson()
                  .toJsonTree(
                      Arrays.asList(
                          "2kinghenryvi",
                          "3kinghenryvi",
                          "allswellthatendswell",
                          "hamlet",
                          "juliuscaesar",
                          "kinghenryv",
                          "kinglear",
                          "periclesprinceoftyre",
                          "troilusandcressida"))
                  .getAsJsonArray());
    }
  }

  @Test
  public void testReadFromQuery_nomMterializationDataset() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadFromQueryIntegrationTestBase::readQueryApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "NO_MATERIALIZATION_DATASET"));

    JsonArray jobInfos = result.getAsJsonArray("jobInfos");
    assertThat(jobInfos.size()).isEqualTo(1);
    JsonObject jobInfo = jobInfos.get(0).getAsJsonObject();
    assertThat(jobInfo.get("type").getAsString()).isEqualTo("QUERY");
  }

  @Test
  public void testReadFromQuery() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadFromQueryIntegrationTestBase::readQueryApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "STANDARD"));

    JsonArray jobInfos = result.getAsJsonArray("jobInfos");
    assertThat(jobInfos.size()).isEqualTo(1);
    JsonObject jobInfo = jobInfos.get(0).getAsJsonObject();
    assertThat(jobInfo.get("type").getAsString()).isEqualTo("QUERY");
  }

  @Test
  public void testReadFromQueryWithNewLine() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadFromQueryIntegrationTestBase::readQueryApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "NEW_LINE"));

    JsonArray jobInfos = result.getAsJsonArray("jobInfos");
    assertThat(jobInfos.size()).isEqualTo(1);
    JsonObject jobInfo = jobInfos.get(0).getAsJsonObject();
    assertThat(jobInfo.get("type").getAsString()).isEqualTo("QUERY");
  }

  @Test
  public void testQueryOption() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadFromQueryIntegrationTestBase::readQueryApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of(
                "scenario", "QUERY_OPTION", "isDsv2", String.valueOf(isDsv2OnSpark3AndAbove)));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("schemaMatches").getAsBoolean()).isTrue();
    assertThat(result.get("noBqcTables").getAsBoolean()).isTrue();
    validateResult(result);
  }

  @Test
  public void testMaterializtionToAutoGeneratedTable() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadFromQueryIntegrationTestBase::readQueryApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "AUTO_GENERATED_TABLE"));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    assertThat(result.get("schemaMatches").getAsBoolean()).isTrue();
    validateResult(result);
  }

  @Test
  public void testBadQuery() {
    assertThrows(
        RuntimeException.class,
        () -> {
          testRunner.run(
              ReadFromQueryIntegrationTestBase::readQueryApp,
              testDataset.toString(),
              testTable,
              ImmutableMap.of("scenario", "BAD_QUERY"));
        });
  }

  @Test
  public void testQueryJobPriority() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadFromQueryIntegrationTestBase::readQueryApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "PRIORITY"));

    assertThat(result.get("status").getAsString()).isEqualTo("success");
    validateResult(result);
  }

  @Test
  public void testReadFromLongQueryWithBigQueryJobTimeout() {
    assertThrows(
        RuntimeException.class,
        () -> {
          testRunner.run(
              ReadFromQueryIntegrationTestBase::readQueryApp,
              testDataset.toString(),
              testTable,
              ImmutableMap.of("scenario", "TIMEOUT"));
        });
  }

  @Test
  public void testReadWithNamedParameters() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadFromQueryIntegrationTestBase::readQueryApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "NAMED_PARAMETERS"));

    JsonArray jobInfos = result.getAsJsonArray("jobInfos");
    assertThat(jobInfos.size()).isEqualTo(1);
    JsonObject jobInfo = jobInfos.get(0).getAsJsonObject();
    assertThat(jobInfo.get("type").getAsString()).isEqualTo("QUERY");
    assertThat(jobInfo.get("query").getAsString()).contains("WHERE corpus = @corpus");
    assertThat(result.get("count").getAsLong()).isEqualTo(12L);
    assertThat(result.getAsJsonArray("schemaColumns"))
        .containsExactlyElementsIn(
            new Gson().toJsonTree(Arrays.asList("word", "word_count")).getAsJsonArray());
  }

  @Test
  public void testReadWithPositionalParameters() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadFromQueryIntegrationTestBase::readQueryApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "POSITIONAL_PARAMETERS"));

    JsonArray jobInfos = result.getAsJsonArray("jobInfos");
    assertThat(jobInfos.size()).isEqualTo(1);
    JsonObject jobInfo = jobInfos.get(0).getAsJsonObject();
    assertThat(jobInfo.get("type").getAsString()).isEqualTo("QUERY");
    assertThat(jobInfo.get("query").getAsString()).contains("WHERE corpus = ?");
  }

  @Test
  public void testReadWithMixedParametersFails() {
    Exception thrown =
        assertThrows(
            Exception.class,
            () -> {
              testRunner.run(
                  ReadFromQueryIntegrationTestBase::readQueryApp,
                  testDataset.toString(),
                  testTable,
                  ImmutableMap.of("scenario", "MIXED_PARAMETERS_FAILS"));
            });

    Throwable cause = thrown;
    if (cause instanceof ProvisionException && cause.getCause() != null) {
      cause = cause.getCause();
    }

    assertThat(cause).isInstanceOf(IllegalArgumentException.class);
    assertThat(cause)
        .hasMessageThat()
        .contains("Cannot mix NamedParameters.* and PositionalParameters.* options.");
  }

  @Test
  public void testReadFromQueryWithKmsKey() throws Exception {
    JsonObject result =
        testRunner.run(
            ReadFromQueryIntegrationTestBase::readQueryApp,
            testDataset.toString(),
            testTable,
            ImmutableMap.of("scenario", "KMS_KEY"));

    JsonArray jobInfos = result.getAsJsonArray("jobInfos");
    assertThat(jobInfos.size()).isEqualTo(1);
    JsonObject jobInfo = jobInfos.get(0).getAsJsonObject();
    assertThat(jobInfo.get("type").getAsString()).isEqualTo("QUERY");
    String envKmsKey = System.getenv("BIGQUERY_KMS_KEY_NAME");
    String kmsKeyName =
        envKmsKey != null ? envKmsKey : "projects/p/locations/l/keyRings/k/cryptoKeys/c";
    assertThat(jobInfo.get("kmsKeyName").getAsString())
        .isEqualTo(kmsKeyName + "/cryptoKeyVersions/1");
  }
}

class TestBigQueryJobCompletionListener extends SparkListener {

  private List<JobInfo> jobInfos = new java.util.concurrent.CopyOnWriteArrayList<>();

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    if (event instanceof BigQueryJobCompletedEvent) {
      jobInfos.add(((BigQueryJobCompletedEvent) event).getJobInfo());
    }
  }

  public ImmutableList<JobInfo> getJobInfos() {
    return ImmutableList.copyOf(jobInfos);
  }

  public void reset() {
    jobInfos.clear();
  }
}
