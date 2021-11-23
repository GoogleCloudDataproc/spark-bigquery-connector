/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.io.Serializable;

public class GenericDataSourceHelperClass implements Serializable {

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
