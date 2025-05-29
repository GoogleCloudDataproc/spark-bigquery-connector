/*
 * Copyright 2025 Google Inc. All Rights Reserved.
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

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class TestDataset extends ExternalResource {

  String testDataset;

  @Override
  public Statement apply(Statement base, Description description) {
    // using hex string to shorten the dataset name
    this.testDataset =
        String.format(
            "spark_bq_%x_%x_%x_%x",
            System.currentTimeMillis(),
            System.nanoTime(),
            description.getClassName().hashCode(),
            description.getMethodName().hashCode());
    return super.apply(base, description);
  }

  @Override
  protected void before() throws Throwable {
    IntegrationTestUtils.createDataset(testDataset);
    IntegrationTestUtils.runQuery(
        String.format(
            TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE,
            testDataset,
            TestConstants.ALL_TYPES_TABLE_NAME));
    IntegrationTestUtils.createView(testDataset, TestConstants.SHAKESPEARE_VIEW);
    IntegrationTestUtils.runQuery(
        String.format(
            TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_QUERY_TEMPLATE,
            testDataset,
            TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_NAME));
    IntegrationTestUtils.runQuery(
        String.format(
            TestConstants.DIFF_IN_SCHEMA_SRC_TABLE,
            testDataset,
            TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_NAME));
    IntegrationTestUtils.runQuery(
        String.format(
            TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_WITH_DESCRIPTION,
            testDataset,
            TestConstants.DIFF_IN_SCHEMA_SRC_TABLE_NAME_WITH_DESCRIPTION));
  }

  @Override
  protected void after() {
    IntegrationTestUtils.deleteDatasetAndTables(testDataset);
  }

  @Override
  public String toString() {
    return testDataset;
  }
}
