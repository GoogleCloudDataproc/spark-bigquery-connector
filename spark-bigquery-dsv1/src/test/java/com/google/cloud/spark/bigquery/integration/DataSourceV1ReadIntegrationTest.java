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

import com.google.cloud.bigquery.connector.common.AccessTokenProvider;
import com.google.cloud.bigquery.connector.common.integration.DefaultCredentialsDelegateGcloudCredentialsProvider;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class DataSourceV1ReadIntegrationTest extends ReadIntegrationTestBase {

  // @TODO Move to support class once DSv2 supports all types
  @Test
  public void testReadDataTypes() {
    Dataset<Row> allTypesTable = readAllTypesTable();
    Row expectedValues =
        spark
            .range(1)
            .select(TestConstants.ALL_TYPES_TABLE_COLS.stream().toArray(Column[]::new))
            .head();
    Row row = allTypesTable.head();

    IntegrationTestUtils.compareRows(row, expectedValues);
  }

  @Test
  public void testCustomCredentialsProvider() throws Exception {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("credentialsProvider", DefaultCredentialsDelegateGcloudCredentialsProvider.class.getCanonicalName())
            .load(TestConstants.SHAKESPEARE_TABLE);
    assertThat(df.count()).isEqualTo(TestConstants.SHAKESPEARE_TABLE_NUM_ROWS);
  }

  @Test
  public void testCustomAccessTokenProvider() throws Exception {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("gcpAccessTokenProvider", AccessTokenProvider.class.getCanonicalName())
            .load(TestConstants.SHAKESPEARE_TABLE);
    assertThat(df.count()).isEqualTo(TestConstants.SHAKESPEARE_TABLE_NUM_ROWS);
  }

  // additional tests are from the super-class

}
