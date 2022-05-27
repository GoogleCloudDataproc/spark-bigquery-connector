package com.google.cloud.spark.bigquery.acceptance;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.longrunning.OperationSnapshot;
import java.util.Arrays;
import org.junit.Test;

/** Tests basic functionality of the connector by reading a BigQuery table */
public class ReadSheakspeareDataprocServerlessAcceptanceTestBase
    extends DataprocServerlessAcceptanceTestBase {

  public ReadSheakspeareDataprocServerlessAcceptanceTestBase(String connectorJarPrefix) {
    super(connectorJarPrefix);
  }

  @Test
  public void testBatch() throws Exception {
    OperationSnapshot operationSnapshot =
        createAndRunPythonBatch(
            context,
            testName,
            "read_shakespeare.py",
            null,
            Arrays.asList(context.getResultsDirUri(testName)),
            480);
    assertThat(operationSnapshot.isDone()).isTrue();
    assertThat(operationSnapshot.getErrorMessage()).isEmpty();
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo("spark,10");
  }
}
