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

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.longrunning.OperationSnapshot;
import java.util.Arrays;
import org.junit.Test;

/** Tests basic functionality of the connector by reading a BigQuery table */
public class ReadSheakspeareDataprocServerlessAcceptanceTestBase
    extends DataprocServerlessAcceptanceTestBase {

  public ReadSheakspeareDataprocServerlessAcceptanceTestBase(
      String connectorJarPrefix, String s8sImageVersion) {
    super(connectorJarPrefix, s8sImageVersion);
  }

  @Test
  public void testBatch() throws Exception {
    OperationSnapshot operationSnapshot =
        createAndRunPythonBatch(
            context,
            testName,
            "read_shakespeare.py",
            null,
            Arrays.asList(context.getResultsDirUri(testName)));
    assertThat(operationSnapshot.isDone()).isTrue();
    assertThat(operationSnapshot.getErrorMessage()).isEmpty();
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo("spark,10");
  }
}
