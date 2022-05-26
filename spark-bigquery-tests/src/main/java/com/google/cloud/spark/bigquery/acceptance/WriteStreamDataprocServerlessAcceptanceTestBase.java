package com.google.cloud.spark.bigquery.acceptance;

import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.getNumOfRowsOfBqTable;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.longrunning.OperationSnapshot;
import java.util.Arrays;
import org.junit.Test;

/** Test the writeStream support on an actual cluster. */
public class WriteStreamDataprocServerlessAcceptanceTestBase
    extends DataprocServerlessAcceptanceTestBase {

  public WriteStreamDataprocServerlessAcceptanceTestBase(String connectorJarPrefix) {
    super(connectorJarPrefix);
  }

  @Test
  public void testBatch() throws Exception {
    String testName = "write-stream-test";
    String jsonFileName = "write_stream_data.json";
    String jsonFileUri = context.testBaseGcsDir + "/" + testName + "/json/" + jsonFileName;

    AcceptanceTestUtils.uploadToGcs(
        getClass().getResourceAsStream("/acceptance/" + jsonFileName),
        jsonFileUri,
        "application/json");

    OperationSnapshot operationSnapshot =
        createAndRunPythonBatch(
            context,
            testName,
            "write_stream.py",
            null,
            Arrays.asList(
                context.testBaseGcsDir + "/" + testName + "/json/",
                context.bqDataset,
                context.bqStreamTable,
                AcceptanceTestUtils.BUCKET),
            480);
    assertThat(operationSnapshot.isDone()).isTrue();
    assertThat(operationSnapshot.getErrorMessage()).isEmpty();

    int numOfRows = getNumOfRowsOfBqTable(context.bqDataset, context.bqStreamTable);
    assertThat(numOfRows).isEqualTo(2);
  }
}
