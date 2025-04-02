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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.spark.bigquery.events.BigQueryJobCompletedEvent;
import com.google.common.collect.ImmutableList;
import com.google.inject.ProvisionException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

class ReadFromQueryIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

  private BigQuery bq;

  private final boolean isDsv2OnSpark3AndAbove;

  private TestBigQueryJobCompletionListener listener = new TestBigQueryJobCompletionListener();

  @Before
  public void addListener() {
    listener.reset();
    spark.sparkContext().addSparkListener(listener);
  }

  @After
  public void removeListener() {
    spark.sparkContext().removeSparkListener(listener);
  }

  protected ReadFromQueryIntegrationTestBase() {
    this(false);
  }

  protected ReadFromQueryIntegrationTestBase(boolean isDsv2OnSpark3AndAbove) {
    super();
    this.bq = BigQueryOptions.getDefaultInstance().getService();
    this.isDsv2OnSpark3AndAbove = isDsv2OnSpark3AndAbove;
  }

  private void testReadFromQueryInternal(String query) {
    Dataset<Row> df = spark.read().format("bigquery").option("viewsEnabled", true).load(query);

    validateResult(df);
    // validate event publishing
    List<JobInfo> jobInfos = listener.getJobInfos();
    assertThat(jobInfos).hasSize(1);
    JobInfo jobInfo = jobInfos.iterator().next();
    assertThat(((QueryJobConfiguration) jobInfo.getConfiguration()).getQuery()).isEqualTo(query);
  }

  @Test
  public void testReadFromQuery() {
    // the query suffix is to make sure that each format will have
    // a different table created due to the destination table cache
    String random = String.valueOf(System.nanoTime());
    String query =
        String.format(
            "SELECT corpus, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE word='spark' AND '%s'='%s'",
            random, random);
    testReadFromQueryInternal(query);
  }

  @Test
  public void testReadFromQueryWithNewLine() {
    // the query suffix is to make sure that each format will have
    // a different table created due to the destination table cache
    String random = String.valueOf(System.nanoTime());
    String query =
        String.format(
            "SELECT corpus, word_count FROM `bigquery-public-data.samples.shakespeare`\n"
                + "WHERE word='spark' AND '%s'='%s'",
            random, random);
    testReadFromQueryInternal(query);
  }

  @Test
  public void testQueryOption() {
    // the query suffix is to make sure that each format will have
    // a different table created due to the destination table cache
    String random = String.valueOf(System.nanoTime());
    String query =
        String.format(
            "SELECT corpus, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE word='spark' AND '%s'='%s'",
            random, random);
    Dataset<Row> df =
        spark.read().format("bigquery").option("viewsEnabled", true).option("query", query).load();

    StructType expectedSchema =
        DataTypes.createStructType(
            ImmutableList.of(
                DataTypes.createStructField("corpus", DataTypes.StringType, true),
                DataTypes.createStructField("word_count", DataTypes.LongType, true)));

    assertThat(df.schema()).isEqualTo(expectedSchema);

    if (isDsv2OnSpark3AndAbove) {
      Iterable<Table> tablesInDataset =
          IntegrationTestUtils.listTables(
              DatasetId.of(testDataset.toString()),
              TableDefinition.Type.TABLE,
              TableDefinition.Type.MATERIALIZED_VIEW);
      assertThat(
              StreamSupport.stream(tablesInDataset.spliterator(), false)
                  .noneMatch(table -> table.getTableId().getTable().startsWith("_bqc_")))
          .isTrue();
    }

    validateResult(df);
  }

  private void validateResult(Dataset<Row> df) {
    long totalRows = df.count();
    assertThat(totalRows).isEqualTo(9);

    List<String> corpuses = df.select("corpus").as(Encoders.STRING()).collectAsList();
    List<String> expectedCorpuses =
        Arrays.asList(
            "2kinghenryvi",
            "3kinghenryvi",
            "allswellthatendswell",
            "hamlet",
            "juliuscaesar",
            "kinghenryv",
            "kinglear",
            "periclesprinceoftyre",
            "troilusandcressida");
    assertThat(corpuses).containsExactlyElementsIn(expectedCorpuses);
  }

  @Test
  public void testBadQuery() {
    String badSql = "SELECT bogus_column FROM `bigquery-public-data.samples.shakespeare`";
    // v1 throws BigQueryConnectorException
    // v2 throws Guice ProviderException, as the table is materialized in teh module
    assertThrows(
        RuntimeException.class,
        () -> {
          spark.read().format("bigquery").option("viewsEnabled", true).load(badSql);
        });
  }

  @Test
  public void testQueryJobPriority() {
    String random = String.valueOf(System.nanoTime());
    String query =
        String.format(
            "SELECT corpus, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE word='spark' AND '%s'='%s'",
            random, random);
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("viewsEnabled", true)
            .option("queryJobPriority", "batch")
            .load(query);

    validateResult(df);
  }

  @Test
  public void testReadFromLongQueryWithBigQueryJobTimeout() {
    String query = "SELECT * FROM `largesamples.wikipedia_pageviews_201001`";
    assertThrows(
        RuntimeException.class,
        () -> {
          try {
            spark
                .read()
                .format("bigquery")
                .option("viewsEnabled", true)
                .option("bigQueryJobTimeoutInMinutes", "1")
                .load(query)
                .show();
          } catch (Exception e) {
            throw e;
          }
        });
  }

  @Test
  public void testReadWithNamedParameters() {
    listener.reset();
    // Hardcoded named parameter query
    String namedParamQuery =
        "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare`" +
            " WHERE corpus = @corpus AND word_count >= @min_word_count ORDER BY word_count DESC";

    Dataset<Row> df = spark
        .read()
        .format("bigquery")
        .option("query", namedParamQuery) // Use hardcoded query
        .option("viewsEnabled", "true")
        .option("NamedParameters.corpus", "STRING:romeoandjuliet")
        .option("NamedParameters.min_word_count", "INT64:250")
        .load();

    // Hardcoded expected schema
    StructType expectedSchema =
        DataTypes.createStructType(
            ImmutableList.of(
                DataTypes.createStructField("word", DataTypes.StringType, true),
                DataTypes.createStructField("word_count", DataTypes.LongType, true)));
    assertThat(df.schema()).isEqualTo(expectedSchema);

    assertThat(df.count()).isGreaterThan(0L);

    List<JobInfo> jobInfos = listener.getJobInfos();
    assertThat(jobInfos).hasSize(1);
    JobInfo jobInfo = jobInfos.iterator().next();
    assertThat(jobInfo.getConfiguration().getType()).isEqualTo(JobConfiguration.Type.QUERY);
    QueryJobConfiguration queryConfig = jobInfo.getConfiguration();
    assertThat(queryConfig.getQuery()).isEqualTo(namedParamQuery);
  }

  @Test
  public void testReadWithPositionalParameters() {
    listener.reset();
    // Hardcoded positional parameter query
    String positionalParamQuery =
        "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare`" +
            " WHERE corpus = ? AND word_count >= ? ORDER BY word_count DESC";

    Dataset<Row> df = spark
        .read()
        .format("bigquery")
        .option("query", positionalParamQuery) // Use hardcoded query
        .option("viewsEnabled", "true")
        .option("PositionalParameters.1", "STRING:romeoandjuliet")
        .option("PositionalParameters.2", "INT64:250")
        .load();

    // Hardcoded expected schema
    StructType expectedSchema =
        DataTypes.createStructType(
            ImmutableList.of(
                DataTypes.createStructField("word", DataTypes.StringType, true),
                DataTypes.createStructField("word_count", DataTypes.LongType, true)));
    assertThat(df.schema()).isEqualTo(expectedSchema);

    assertThat(df.count()).isGreaterThan(0L);

    List<JobInfo> jobInfos = listener.getJobInfos();
    assertThat(jobInfos).hasSize(1);
    JobInfo jobInfo = jobInfos.iterator().next();
    assertThat(jobInfo.getConfiguration().getType()).isEqualTo(JobConfiguration.Type.QUERY);
    QueryJobConfiguration queryConfig = jobInfo.getConfiguration();
    assertThat(queryConfig.getQuery()).isEqualTo(positionalParamQuery); // Compare with hardcoded query
    // Assertions for specific positional parameters in JobInfo if needed/possible
  }

  @Test
  public void testReadWithMixedParametersFails() {
    // Hardcoded named parameter query (could be positional too, doesn't matter for the failure)
    String queryForFailure =
        "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare`" +
            " WHERE corpus = @corpus AND word_count >= @min_word_count ORDER BY word_count DESC";

    Exception thrown = assertThrows(Exception.class,
        () -> {
          spark
              .read()
              .format("bigquery")
              .option("query", queryForFailure) // Use hardcoded query
              .option("viewsEnabled", "true")
              .option("NamedParameters.corpus", "STRING:whatever")
              .option("PositionalParameters.1", "INT64:100")
              .load()
              .show();
        });

    Throwable cause = thrown;
    if (cause instanceof ProvisionException && cause.getCause() != null) {
      cause = cause.getCause();
    }

    assertThat(cause).isInstanceOf(IllegalArgumentException.class);
    assertThat(cause)
        .hasMessageThat()
        .contains("Cannot mix NamedParameters.* and PositionalParameters.* options.");

    assertThat(listener.getJobInfos()).isEmpty();
  }
}

class TestBigQueryJobCompletionListener extends SparkListener {

  private List<JobInfo> jobInfos = new ArrayList<>();

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
