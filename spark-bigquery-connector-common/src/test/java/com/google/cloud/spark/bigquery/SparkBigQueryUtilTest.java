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
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
}
