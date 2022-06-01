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

  public BigNumericDataprocServerlessAcceptanceTestBase(String connectorJarPrefix) {
    super(connectorJarPrefix);
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
            Arrays.asList(tableName, context.getResultsDirUri(testName)),
            480);
    assertThat(operationSnapshot.isDone()).isTrue();
    assertThat(operationSnapshot.getErrorMessage()).isEmpty();
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo(MIN_BIG_NUMERIC + "," + MAX_BIG_NUMERIC);
  }
}
