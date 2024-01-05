/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.spark.bigquery;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigLakeConfiguration;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.types.Metadata;
import org.junit.Test;

public class SparkBigQueryUtilTest {

  private SQLConf sqlConf;

  @Test
  public void testGetJobIdInternal_hasTagsAndAppId() {
    String jobId =
        SparkBigQueryUtil.getJobIdInternal(
            "dataproc_hash_2cc09905-1a77-3473-9070-d067ea047f4f,dataproc_job_56324553ed9110908c83b6317f4faab,dataproc_master_index_0,dataproc_uuid_4342a069-0b69-504e-bea4-44986422f720",
            "application_1646959792249_0001");
    assertThat(jobId).isEqualTo("dataproc_job_56324553ed9110908c83b6317f4faab");
  }

  @Test
  public void testGetJobIdInternal_missingTags_hasAppId() {
    String jobId = SparkBigQueryUtil.getJobIdInternal("missing", "application_1646959792249_0001");
    assertThat(jobId).isEqualTo("application_1646959792249_0001");
  }

  @Test
  public void testGetJobIdInternal_missingBoth() {
    String jobId = SparkBigQueryUtil.getJobIdInternal("missing", "");
    assertThat(jobId).isEqualTo("");
  }

  @Test
  public void testSaveModeToWriteDisposition() {
    assertThat(SparkBigQueryUtil.saveModeToWriteDisposition(SaveMode.ErrorIfExists))
        .isEqualTo(JobInfo.WriteDisposition.WRITE_EMPTY);
    assertThat(SparkBigQueryUtil.saveModeToWriteDisposition(SaveMode.Append))
        .isEqualTo(JobInfo.WriteDisposition.WRITE_APPEND);
    assertThat(SparkBigQueryUtil.saveModeToWriteDisposition(SaveMode.Ignore))
        .isEqualTo(JobInfo.WriteDisposition.WRITE_APPEND);
    assertThat(SparkBigQueryUtil.saveModeToWriteDisposition(SaveMode.Overwrite))
        .isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);
    assertThrows(
        IllegalArgumentException.class, () -> SparkBigQueryUtil.saveModeToWriteDisposition(null));
  }

  @Test
  public void testParseSimpleTableId() {
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
    ImmutableMap<String, String> options = ImmutableMap.of("table", "dataset.table");
    TableId tableId = SparkBigQueryUtil.parseSimpleTableId(spark, options);
    assertThat(tableId.getDataset()).isEqualTo("dataset");
    assertThat(tableId.getTable()).isEqualTo("table");
  }

  @Test
  public void testIsJson_SqlTypeJson() {
    Metadata metadata = Metadata.fromJson("{\"sqlType\":\"JSON\"}");
    assertThat(SparkBigQueryUtil.isJson(metadata)).isTrue();
  }

  @Test
  public void testIsJson_SqlTypeOther() {
    Metadata metadata = Metadata.fromJson("{\"sqlType\":\"OTHER\"}");
    assertThat(SparkBigQueryUtil.isJson(metadata)).isFalse();
  }

  @Test
  public void testIsJson_NoSqlType() {
    Metadata metadata = Metadata.empty();
    assertThat(SparkBigQueryUtil.isJson(metadata)).isFalse();
  }

  @Test
  public void testExtractPartitionFilters_no_match() {
    ImmutableList<Filter> filters =
        SparkBigQueryUtil.extractPartitionAndClusteringFilters(
            TableInfo.of(
                TableId.of("dataset", "table"),
                StandardTableDefinition.newBuilder()
                    .setTimePartitioning(
                        TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                            .setField("foo")
                            .build())
                    .build()),
            ImmutableList.of(IsNotNull.apply("bar")));
    assertThat(filters).isEmpty();
  }

  @Test
  public void testExtractPartitionFilters_has_match() {
    ImmutableList<Filter> filters =
        SparkBigQueryUtil.extractPartitionAndClusteringFilters(
            TableInfo.of(
                TableId.of("dataset", "table"),
                StandardTableDefinition.newBuilder()
                    .setTimePartitioning(
                        TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                            .setField("foo")
                            .build())
                    .build()),
            ImmutableList.of(IsNotNull.apply("foo")));
    assertThat(filters).hasSize(1);
    assertThat(filters.get(0).references()).asList().containsExactly("foo");
  }

  @Test
  public void testExtractJobLabels_no_labels() {
    ImmutableMap<String, String> labels = SparkBigQueryUtil.extractJobLabels(new SparkConf());
    assertThat(labels).isEmpty();
  }

  @Test
  public void testExtractJobLabels_with_labels() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set(
        "spark.yarn.tags",
        "dataproc_hash_d371badb-9112-3812-8284-ee81f54d3558,dataproc_job_d8f27392957446dbbd7dc28df568e4eb,dataproc_master_index_0,dataproc_uuid_df379ef3-eeda-3234-8941-e1a36a1959a3");
    ImmutableMap<String, String> labels = SparkBigQueryUtil.extractJobLabels(sparkConf);
    assertThat(labels).hasSize(2);
    assertThat(labels).containsEntry("dataproc_job_id", "d8f27392957446dbbd7dc28df568e4eb");
    assertThat(labels).containsEntry("dataproc_job_uuid", "df379ef3-eeda-3234-8941-e1a36a1959a3");
  }

  @Test
  public void testIsBigLakeManagedTable_with_BigLakeManagedTable() {
    TableInfo bigLakeManagedTable =
        TableInfo.of(
            TableId.of("dataset", "biglakemanagedtable"),
            StandardTableDefinition.newBuilder()
                .setBigLakeConfiguration(
                    BigLakeConfiguration.newBuilder()
                        .setTableFormat("ICEBERG")
                        .setConnectionId("us-connection")
                        .setFileFormat("PARQUET")
                        .setStorageUri("gs://bigquery/blmt/nations.parquet")
                        .build())
                .build());

    assertTrue(SparkBigQueryUtil.isBigLakeManagedTable(bigLakeManagedTable));
  }

  @Test
  public void testIsBigLakeManagedTable_with_BigQueryExternalTable() {
    TableInfo bigQueryExternalTable =
        TableInfo.of(
            TableId.of("dataset", "bigqueryexternaltable"),
            ExternalTableDefinition.newBuilder(
                    "gs://bigquery/nations.parquet", FormatOptions.avro())
                .build());

    assertFalse(SparkBigQueryUtil.isBigLakeManagedTable(bigQueryExternalTable));
  }

  @Test
  public void testIsBigLakeManagedTable_with_BigQueryNativeTable() {
    TableInfo bigQueryNativeTable =
        TableInfo.of(
            TableId.of("dataset", "bigquerynativetable"),
            StandardTableDefinition.newBuilder().setLocation("us-east-1").build());

    assertFalse(SparkBigQueryUtil.isBigLakeManagedTable(bigQueryNativeTable));
  }

  @Test
  public void testIsBigQueryNativeTable_with_BigLakeManagedTable() {
    TableInfo bigLakeManagedTable =
        TableInfo.of(
            TableId.of("dataset", "biglakemanagedtable"),
            StandardTableDefinition.newBuilder()
                .setBigLakeConfiguration(
                    BigLakeConfiguration.newBuilder()
                        .setTableFormat("ICEBERG")
                        .setConnectionId("us-connection")
                        .setFileFormat("PARQUET")
                        .setStorageUri("gs://bigquery/blmt/nations.parquet")
                        .build())
                .build());

    assertFalse(SparkBigQueryUtil.isBigQueryNativeTable(bigLakeManagedTable));
  }

  @Test
  public void testIsBigQueryNativeTable_with_BigQueryExternalTable() {
    TableInfo bigQueryExternalTable =
        TableInfo.of(
            TableId.of("dataset", "bigqueryexternaltable"),
            ExternalTableDefinition.newBuilder(
                    "gs://bigquery/nations.parquet", FormatOptions.avro())
                .build());

    assertFalse(SparkBigQueryUtil.isBigQueryNativeTable(bigQueryExternalTable));
  }

  @Test
  public void testIsBigQueryNativeTable_with_BigQueryNativeTable() {
    TableInfo bigQueryNativeTable =
        TableInfo.of(
            TableId.of("dataset", "bigquerynativetable"),
            StandardTableDefinition.newBuilder().setLocation("us-east-1").build());

    assertTrue(SparkBigQueryUtil.isBigQueryNativeTable(bigQueryNativeTable));
  }
}
