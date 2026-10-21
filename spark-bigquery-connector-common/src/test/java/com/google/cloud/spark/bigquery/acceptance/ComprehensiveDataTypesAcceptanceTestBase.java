/*
 * Copyright 2024 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.acceptance;

import static com.google.common.truth.Truth.assertWithMessage;

import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobStatus;
import java.util.Arrays;
import org.junit.Test;

/**
 * Comprehensive acceptance tests for BigQuery data types and casing scenarios.
 *
 * <p>Test Methodology (Read-Verify-Write-Verify Cycle):
 *
 * <p><b>Phase 1: Creating the "Gold Standard" (BigQuery Native)</b>
 *
 * <p>Before Spark ever touches the data, we establish a perfect "Source of Truth" using BigQuery's
 * own engine.
 *
 * <ol>
 *   <li><b>Segregation:</b> Java creates two distinct source tables:
 *       <ul>
 *         <li><b>Standard Table:</b> Contains 13 types (INT64, STRING, NUMERIC, TIMESTAMP, etc.)
 *             that are compatible with both Parquet and Avro.
 *         <li><b>Special Table:</b> Contains only JSON and MapType (Repeated Structs). This
 *             isolates complex types that have specific documentation requirements (like Avro-only
 *             for Indirect writes).
 *       </ul>
 *   <li><b>DDL Execution:</b> Java uses the BigQuery client to run a <code>
 *       CREATE OR REPLACE TABLE ... AS SELECT ...</code> query. This ensures the source data has
 *       exactly 4096 (or 8192) rows and perfect type integrity.
 * </ol>
 *
 * <p><b>Phase 2: Isolated Ingestion Verification (Read Path)</b>
 *
 * <p>This phase verifies that the connector can correctly fetch data into Spark memory.
 *
 * <ol>
 *   <li><b>Format Testing:</b> For every source table created in Phase 1, Java submits a Dataproc
 *       job that reads the table using readDataFormat=ARROW. (Avro read format is skipped due to
 *       known environment-specific ClassCastExceptions in Spark 3.5).
 *   <li><b>Internal PySpark Checks:</b> The script validates:
 *       <ul>
 *         <li><b>Row Count:</b> Does the Spark DataFrame have exactly the same number of rows as
 *             the BigQuery source?
 *         <li><b>Schema:</b> Does Spark correctly map the BigQuery types?
 *         <li><b>Null Verification:</b> Ensures all core fields are SQL NULL (JSON and MapType are
 *             ignored due to inconsistent string/collection representations of NULL).
 *         <li><b>NQE Trigger:</b> Applies a filter (e.g., f_int64 IS NOT NULL) to ensure the Native
 *             Query Engine (Gluten/NQE) doesn't crash during the read.
 *       </ul>
 * </ol>
 *
 * <p><b>Phase 3: Isolated Persistence Verification (Write Path)</b>
 *
 * <p>This phase verifies the "last mile"—getting data from Spark back into BigQuery.
 *
 * <ol>
 *   <li><b>Method Testing:</b> For every source table, Java submits two more jobs to test the two
 *       different write pipelines:
 *       <ul>
 *         <li><b>Scenario 1 (Indirect):</b>
 *             <ul>
 *               <li>Standard Suite: Writes back using Parquet intermediate format.
 *               <li>Special Suite: Writes back using Avro intermediate format + sqlType=JSON
 *                   metadata.
 *             </ul>
 *         <li><b>Scenario 2 (Direct):</b> Uses the BigQuery Storage Write API (Protobuf) for all
 *             types.
 *       </ul>
 *   <li><b>Immediate Round-Trip Check:</b> Once the write finishes, the PySpark script itself
 *       immediately reads the new _back table and compares its row count and content to the
 *       original DataFrame. If there was any data loss during the write, the job fails immediately.
 * </ol>
 *
 * <p><b>Phase 4: Automated Resource Teardown</b>
 *
 * <p>To prevent GCS and BigQuery costs from accumulating:
 *
 * <ol>
 *   <li><b>The Container Pattern:</b> All tables (source, indirect_back, direct_back) are created
 *       inside a single, unique BigQuery dataset generated for that specific test class.
 *   <li><b>Force Delete:</b> When the Java test suite completes (even if it fails), the tearDown()
 *       method force-deletes the entire BigQuery dataset and all temporary GCS artifacts in one
 *       command.
 * </ol>
 *
 * <p><b>Summary of Coverage</b>
 *
 * <p>By the time a single test method (like testSmallBatchArrowNoNulls) finishes, the connector has
 * successfully passed:
 *
 * <ul>
 *   <li>Read Format (Arrow)
 *   <li>2 Write Methods (Indirect, Direct)
 *   <li>2 Intermediate Formats (Parquet, Avro)
 *   <li>15 Data Types (Segregated for compatibility)
 *   <li>Internal Spark Assertions (Python-side)
 *   <li>Distributed Cluster Validation (Dataproc)
 * </ul>
 *
 * <p>Existing Cluster Support:
 *
 * <p>If the <code>DATAPROC_CLUSTER</code> environment variable is set, the suite automatically
 * targets the specified cluster and skips cluster creation and deletion.
 */
public abstract class ComprehensiveDataTypesAcceptanceTestBase extends DataprocAcceptanceTestBase {

  protected static AcceptanceTestContext context;

  public ComprehensiveDataTypesAcceptanceTestBase(AcceptanceTestContext context) {
    super(context);
  }

  /**
   * Verifies that the connector correctly handles a comprehensive set of BigQuery data types at the
   * internal Arrow batch boundary (4096 rows) with no NULLs.
   */
  @Test
  public void testSmallBatchArrowNoNulls() throws Exception {
    runComprehensiveDataTypeTest("small_batch", 4096, false);
  }

  /**
   * Verifies that the connector correctly handles a large batch of rows (8192 rows) with no NULLs,
   * spanning multiple Arrow record batches.
   */
  @Test
  public void testLargeBatchArrowNoNulls() throws Exception {
    runComprehensiveDataTypeTest("large_batch", 8192, false);
  }

  /**
   * Verifies that the connector correctly handles a large batch of rows where all fields are NULL
   * across all supported data types, triggering Arrow batching logic for null-heavy data.
   */
  @Test
  public void testLargeBatchNulls() throws Exception {
    runComprehensiveDataTypeTest("large_nulls", 8192, true);
  }

  /**
   * Verifies that the connector correctly resolves mismatched casing for columns in BOTH
   * directions.
   */
  @Test
  public void testCaseSensitivity() throws Exception {
    runCaseSensitivityScenario("UPPER", "lower");
    runCaseSensitivityScenario("lower", "UPPER");
  }

  /*
   * The following methods are overridden and ignored to prevent standard acceptance tests
   * from running during the comprehensive data type baseline verification.
   */

  @Override
  @org.junit.Ignore
  public void testRead() throws Exception {}

  @Override
  @org.junit.Ignore
  public void testBigNumeric() throws Exception {}

  @Override
  @org.junit.Ignore
  public void writeStream() throws Exception {}

  protected void runComprehensiveDataTypeTest(String prefix, long rowCount, boolean allNulls)
      throws Exception {
    String timestamp = String.valueOf(System.currentTimeMillis());

    // 1. Standard Types Suite (Parquet/Direct compatible)
    runStandardTypesSuite(prefix + "_standard", timestamp, rowCount, allNulls);

    // 2. Special Types Suite (Avro/Direct only: JSON, Map)
    runSpecialTypesSuite(prefix + "_special", timestamp, rowCount, allNulls);
  }

  private void runStandardTypesSuite(
      String prefix, String timestamp, long rowCount, boolean allNulls) throws Exception {
    String table = prefix + "_" + timestamp;
    String fullTableName = context.bqDataset + "." + table;

    createStandardTable(fullTableName, rowCount, allNulls);

    // Read Verification (Standardized on ARROW)
    runReadVerification(fullTableName, allNulls, rowCount);

    // Write Verification
    runWriteVerification(fullTableName, fullTableName + "_indirect_back", "write_standard");
    runDirectWriteVerification(fullTableName, fullTableName + "_direct_back", "write_standard");
  }

  private void runSpecialTypesSuite(
      String prefix, String timestamp, long rowCount, boolean allNulls) throws Exception {
    String table = prefix + "_" + timestamp;
    String fullTableName = context.bqDataset + "." + table;

    createSpecialTable(fullTableName, rowCount, allNulls);

    // Read Verification (Standardized on ARROW)
    runReadVerification(fullTableName, allNulls, rowCount);

    // Write Verification
    runWriteVerification(fullTableName, fullTableName + "_indirect_back", "write_special");
    runDirectWriteVerification(fullTableName, fullTableName + "_direct_back", "write_special");
  }

  private void createStandardTable(String fullTableName, long rowCount, boolean allNulls)
      throws Exception {
    StringBuilder ddl = new StringBuilder();
    ddl.append("CREATE OR REPLACE TABLE `").append(fullTableName).append("` (");
    ddl.append("f_int64 INT64, ");
    ddl.append("f_string STRING, ");
    ddl.append("f_bool BOOL, ");
    ddl.append("f_float64 FLOAT64, ");
    ddl.append("f_bytes BYTES, ");
    ddl.append("f_date DATE, ");
    ddl.append("f_datetime DATETIME, ");
    ddl.append("f_time TIME, ");
    ddl.append("f_timestamp TIMESTAMP, ");
    ddl.append("f_numeric NUMERIC, ");
    ddl.append("f_bignumeric BIGNUMERIC(38, 9), ");
    ddl.append("f_array_int ARRAY<INT64>, ");
    ddl.append("f_struct STRUCT<a INT64, b STRING> ");
    ddl.append(") AS SELECT ");

    if (allNulls) {
      ddl.append("CAST(NULL AS INT64), ");
      ddl.append("CAST(NULL AS STRING), ");
      ddl.append("CAST(NULL AS BOOL), ");
      ddl.append("CAST(NULL AS FLOAT64), ");
      ddl.append("CAST(NULL AS BYTES), ");
      ddl.append("CAST(NULL AS DATE), ");
      ddl.append("CAST(NULL AS DATETIME), ");
      ddl.append("CAST(NULL AS TIME), ");
      ddl.append("CAST(NULL AS TIMESTAMP), ");
      ddl.append("CAST(NULL AS NUMERIC), ");
      ddl.append("CAST(NULL AS BIGNUMERIC), ");
      ddl.append("CAST([] AS ARRAY<INT64>), ");
      ddl.append("CAST(NULL AS STRUCT<a INT64, b STRING>) ");
    } else {
      ddl.append("row_id, ");
      ddl.append("CAST(row_id AS STRING), ");
      ddl.append("MOD(row_id, 2) = 0, ");
      ddl.append("CAST(row_id AS FLOAT64) / 10.0, ");
      ddl.append("CAST('bytes' AS BYTES), ");
      ddl.append("DATE '2023-01-01', ");
      ddl.append("DATETIME '2023-01-01 12:00:00', ");
      ddl.append("TIME '12:00:00', ");
      ddl.append("TIMESTAMP '2023-01-01 12:00:00 UTC', ");
      ddl.append("CAST(row_id AS NUMERIC) / 100.0, ");
      ddl.append("CAST('1.23' AS BIGNUMERIC), ");
      ddl.append("[1, 2, 3], ");
      ddl.append("struct(1 as a, 'b' as b) ");
    }
    ddl.append("FROM UNNEST(GENERATE_ARRAY(1, ").append(rowCount).append(")) as row_id");

    AcceptanceTestUtils.runBqQuery(ddl.toString());
  }

  private void createSpecialTable(String fullTableName, long rowCount, boolean allNulls)
      throws Exception {
    StringBuilder ddl = new StringBuilder();
    ddl.append("CREATE OR REPLACE TABLE `").append(fullTableName).append("` (");
    ddl.append("f_json JSON, ");
    ddl.append("f_map ARRAY<STRUCT<key STRING, value INT64>> ");
    ddl.append(") AS SELECT ");

    if (allNulls) {
      ddl.append("CAST(NULL AS JSON), ");
      ddl.append("CAST([] AS ARRAY<STRUCT<key STRING, value INT64>>) ");
    } else {
      ddl.append("JSON '{\"a\": 1, \"b\": \"c\"}', ");
      ddl.append("[struct('key1' as key, 1 as value), struct('key2' as key, 2 as value)] ");
    }
    ddl.append("FROM UNNEST(GENERATE_ARRAY(1, ").append(rowCount).append(")) as row_id");

    AcceptanceTestUtils.runBqQuery(ddl.toString());
  }

  private void runReadVerification(String table, boolean allNulls, long expectedCount)
      throws Exception {
    Job result =
        createAndRunPythonJob(
            "comprehensive-read",
            "comprehensive_data_types.py",
            null,
            Arrays.asList(
                "read_verify", table, String.valueOf(allNulls), String.valueOf(expectedCount)));

    assertWithMessage("Read Verification failed. Table: %s", table)
        .that(result.getStatus().getState())
        .isEqualTo(JobStatus.State.DONE);
  }

  private void runWriteVerification(String sourceTable, String destinationTable, String mode)
      throws Exception {
    Job result =
        createAndRunPythonJob(
            "comprehensive-write-indirect",
            "comprehensive_data_types.py",
            null,
            Arrays.asList(
                mode, sourceTable, destinationTable, "INDIRECT", AcceptanceTestUtils.BUCKET));

    assertWithMessage(
            "Write Phase failed (INDIRECT). Source: %s, Dest: %s", sourceTable, destinationTable)
        .that(result.getStatus().getState())
        .isEqualTo(JobStatus.State.DONE);
  }

  private void runDirectWriteVerification(String sourceTable, String destinationTable, String mode)
      throws Exception {
    Job result =
        createAndRunPythonJob(
            "comprehensive-write-direct",
            "comprehensive_data_types.py",
            null,
            Arrays.asList(mode, sourceTable, destinationTable, "DIRECT"));

    assertWithMessage(
            "Write Phase failed (DIRECT). Source: %s, Dest: %s", sourceTable, destinationTable)
        .that(result.getStatus().getState())
        .isEqualTo(JobStatus.State.DONE);
  }

  private void runCaseSensitivityScenario(String bqCasing, String sparkCasing) throws Exception {
    String table = "case_" + bqCasing + "_" + System.currentTimeMillis();
    String fullTableName = context.bqDataset + "." + table;
    String columnName = bqCasing.equals("UPPER") ? "TEST_COL" : "test_col";
    String sparkColumn = sparkCasing.equals("UPPER") ? "TEST_COL" : "test_col";

    AcceptanceTestUtils.runBqQuery(
        String.format(
            "CREATE OR REPLACE TABLE `%s` (ID INT64, %s STRING) AS SELECT 1, 'val'",
            fullTableName, columnName));

    Job result =
        createAndRunPythonJob(
            "casing",
            "comprehensive_data_types.py",
            null,
            Arrays.asList("casing", fullTableName, sparkColumn));

    assertWithMessage(
            "Casing scenario failed. BQ: %s, Spark: %s, Table: %s",
            bqCasing, sparkCasing, fullTableName)
        .that(result.getStatus().getState())
        .isEqualTo(JobStatus.State.DONE);
  }
}
