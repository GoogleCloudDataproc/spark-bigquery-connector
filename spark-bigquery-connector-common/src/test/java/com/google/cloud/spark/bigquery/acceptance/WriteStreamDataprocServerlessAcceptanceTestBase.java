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

import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.getNumOfRowsOfBqTable;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.longrunning.OperationSnapshot;
import java.util.Arrays;
import org.junit.Test;

/** Test the writeStream support on an actual cluster. */
public class WriteStreamDataprocServerlessAcceptanceTestBase
    extends DataprocServerlessAcceptanceTestBase {

  public WriteStreamDataprocServerlessAcceptanceTestBase(
      String connectorJarPrefix, String s8sImageVersion) {
    super(connectorJarPrefix, s8sImageVersion);
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
                AcceptanceTestUtils.BUCKET));
    assertThat(operationSnapshot.isDone()).isTrue();
    assertThat(operationSnapshot.getErrorMessage()).isEmpty();

    int numOfRows = getNumOfRowsOfBqTable(context.bqDataset, context.bqStreamTable);
    assertThat(numOfRows).isEqualTo(2);
  }
}
