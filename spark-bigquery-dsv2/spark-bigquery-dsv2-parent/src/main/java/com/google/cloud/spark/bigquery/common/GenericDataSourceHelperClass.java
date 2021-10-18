package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;

public class GenericDataSourceHelperClass {

  public void checkCreateDisposition(SparkBigQueryConfig config) {
    boolean createNever =
        config
            .getCreateDisposition()
            .map(createDisposition -> createDisposition == JobInfo.CreateDisposition.CREATE_NEVER)
            .orElse(false);
    if (createNever) {
      throw new IllegalArgumentException(
          String.format(
              "For table %s Create Disposition is CREATE_NEVER and the table does not exists. Aborting the insert",
              BigQueryUtil.friendlyTableName(config.getTableId())));
    }
  }
}
