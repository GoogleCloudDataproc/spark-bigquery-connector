package com.google.cloud.spark.bigquery;

import static com.google.common.truth.Truth.assertThat;

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
  public void testGetApplicationName_fromConf() {
    SQLConf sqlConf = new SQLConf();
    sqlConf.setConfString("spark.app.name", "test");
    String appName = SparkBigQueryUtil.getApplicationName(sqlConf);
    assertThat(appName).isEqualTo("test");
  }

  @Test
  public void testGetApplicationName_emptyStringInConf() {
    SQLConf sqlConf = new SQLConf();
    sqlConf.setConfString("spark.app.name", "");
    String appName = SparkBigQueryUtil.getApplicationName(sqlConf);
    assertThat(appName).isEqualTo("UnknownSparkApplication");
  }

  @Test
  public void testGetApplicationName_notInConf() {
    SQLConf sqlConf = new SQLConf();
    String appName = SparkBigQueryUtil.getApplicationName(sqlConf);
    assertThat(appName).isEqualTo("UnknownSparkApplication");
  }
}
