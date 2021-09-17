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
package com.google.cloud.spark.bigquery.integration;

import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

public class SparkBigQueryIntegrationTestBase {

  @ClassRule public static SparkFactory sparkFactory = new SparkFactory();
  @ClassRule public static TestDataset testDataset = new TestDataset();

  protected SparkSession spark;
  protected String testTable;

  public SparkBigQueryIntegrationTestBase() {
    this.spark = sparkFactory.spark;
  }

  @Before
  public void createTestTable() {
    testTable = "test_" + System.nanoTime();
  }

  protected static class SparkFactory extends ExternalResource {
    SparkSession spark;

    @Override
    protected void before() throws Throwable {
      spark = SparkSession.builder().master("local").getOrCreate();
      // reducing test's logs
      spark.sparkContext().setLogLevel("WARN");
    }

  }

    protected static class TestDataset extends ExternalResource {

    String testDataset = String
        .format("spark_bigquery_%d_%d", System.currentTimeMillis(), System.nanoTime());

    @Override
    protected void before() throws Throwable {
      IntegrationTestUtils.createDataset(testDataset);
      IntegrationTestUtils.runQuery(String.format(
          TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE,
          testDataset, TestConstants.ALL_TYPES_TABLE_NAME));
      IntegrationTestUtils.createView(testDataset, TestConstants.ALL_TYPES_TABLE_NAME,
          TestConstants.ALL_TYPES_VIEW_NAME);
      IntegrationTestUtils.runQuery(String.format(
          TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_QUERY_TEMPLATE,
          testDataset, TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_NAME));
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
}