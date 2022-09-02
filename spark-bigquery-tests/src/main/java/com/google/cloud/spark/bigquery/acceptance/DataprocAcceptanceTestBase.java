/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.MAX_BIG_NUMERIC;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.MIN_BIG_NUMERIC;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.getNumOfRowsOfBqTable;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.runBqQuery;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.dataproc.v1.*;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.junit.Test;

public class DataprocAcceptanceTestBase extends AcceptanceTestBase {

  protected DataprocAcceptanceTestBase(AcceptanceTestContext context) {
    super(context);
  }

  protected DataprocAcceptanceTestBase(
      AcceptanceTestContext context, boolean sparkStreamingSupported) {
    super(context, sparkStreamingSupported);
  }

  @Test
  public void testRead() throws Exception {
    String testName = "test-read";
    Job result =
        createAndRunPythonJob(
            testName,
            "read_shakespeare.py",
            null,
            Arrays.asList(context.getResultsDirUri(testName)),
            120);
    assertThat(result.getStatus().getState()).isEqualTo(JobStatus.State.DONE);
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo("spark,10");
  }

  @Test
  public void writeStream() throws Exception {
    // TODO: Should be removed once streaming is supported in DSv2
    assumeTrue("Spark streaming is not supported by this connector", sparkStreamingSupported);
    String testName = "write-stream-test";
    String jsonFileName = "write_stream_data.json";
    String jsonFileUri = context.testBaseGcsDir + "/" + testName + "/json/" + jsonFileName;

    AcceptanceTestUtils.uploadToGcs(
        getClass().getResourceAsStream("/acceptance/" + jsonFileName),
        jsonFileUri,
        "application/json");

    Job result =
        createAndRunPythonJob(
            testName,
            "write_stream.py",
            null,
            Arrays.asList(
                context.testBaseGcsDir + "/" + testName + "/json/",
                context.bqDataset,
                context.bqStreamTable,
                AcceptanceTestUtils.BUCKET),
            120);

    assertThat(result.getStatus().getState()).isEqualTo(JobStatus.State.DONE);
    int numOfRows = getNumOfRowsOfBqTable(context.bqDataset, context.bqStreamTable);
    assertThat(numOfRows == 2);
  }

  @Test
  public void testBigNumeric() throws Exception {
    String testName = "test-big-numeric";
    Path pythonLibTargetDir = Paths.get("../../spark-bigquery-python-lib/target");
    Path pythonLibZip =
        AcceptanceTestUtils.getArtifact(pythonLibTargetDir, "spark-bigquery", ".zip");
    String zipFileUri =
        context.testBaseGcsDir + "/" + testName + "/big_numeric_acceptance_test.zip";
    AcceptanceTestUtils.uploadToGcs(
        new FileInputStream(pythonLibZip.toFile()), zipFileUri, "application/zip");

    runBqQuery(
        String.format(
            AcceptanceTestConstants.BIGNUMERIC_TABLE_QUERY_TEMPLATE,
            context.bqDataset,
            context.bqTable));

    String tableName = context.bqDataset + "." + context.bqTable;

    Job result =
        createAndRunPythonJob(
            testName,
            "big_numeric.py",
            zipFileUri,
            Arrays.asList(tableName, context.getResultsDirUri(testName)),
            120);

    assertThat(result.getStatus().getState()).isEqualTo(JobStatus.State.DONE);
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo(MIN_BIG_NUMERIC + "," + MAX_BIG_NUMERIC);
  }
}
