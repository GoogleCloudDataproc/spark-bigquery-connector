package com.google.cloud.spark.bigquery;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
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
}
