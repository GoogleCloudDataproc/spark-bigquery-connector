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
package com.google.cloud.spark.bigquery.acceptance;

import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.MAX_BIG_NUMERIC;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.MIN_BIG_NUMERIC;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.runBqQuery;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.longrunning.OperationSnapshot;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.junit.Test;

/**
 * Tests the correct behavior of the python support lib and the BigNumeric python custom data type
 */
public class BigNumericDataprocServerlessAcceptanceTestBase
    extends DataprocServerlessAcceptanceTestBase {

  public BigNumericDataprocServerlessAcceptanceTestBase(
      String connectorJarPrefix, String s8sImageVersion) {
    super(connectorJarPrefix, s8sImageVersion);
  }

  @Test
  public void testBatch() throws Exception {
    Path pythonLibTargetDir = Paths.get("../../spark-bigquery-python-lib/target");
    Path pythonLibZip =
        AcceptanceTestUtils.getArtifact(pythonLibTargetDir, "spark-bigquery", ".zip");
    String zipFileUri =
        context.testBaseGcsDir + "/" + testName + "/big_numeric_acceptance_test.zip";
    try (InputStream pythonLib = new FileInputStream(pythonLibZip.toFile())) {
      AcceptanceTestUtils.uploadToGcs(pythonLib, zipFileUri, "application/zip");
    }

    runBqQuery(
        String.format(
            AcceptanceTestConstants.BIGNUMERIC_TABLE_QUERY_TEMPLATE,
            context.bqDataset,
            context.bqTable));
    String tableName = context.bqDataset + "." + context.bqTable;

    OperationSnapshot operationSnapshot =
        createAndRunPythonBatch(
            context,
            testName,
            "big_numeric.py",
            zipFileUri,
            Arrays.asList(tableName, context.getResultsDirUri(testName)));
    assertThat(operationSnapshot.isDone()).isTrue();
    assertThat(operationSnapshot.getErrorMessage()).isEmpty();
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo(MIN_BIG_NUMERIC + "," + MAX_BIG_NUMERIC);
  }
}
