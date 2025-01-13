package com.google.cloud.spark.bigquery.integration;

import org.junit.rules.ExternalResource;

public class TestDataset extends ExternalResource {

    String testDataset =
            String.format("spark_bigquery_%d_%d", System.currentTimeMillis(), System.nanoTime());

    @Override
    protected void before() throws Throwable {
        IntegrationTestUtils.createDataset(testDataset);
        IntegrationTestUtils.runQuery(
                String.format(
                        TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE,
                        testDataset,
                        TestConstants.ALL_TYPES_TABLE_NAME));
        IntegrationTestUtils.createView(testDataset, TestConstants.ALL_TYPES_VIEW_NAME);
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