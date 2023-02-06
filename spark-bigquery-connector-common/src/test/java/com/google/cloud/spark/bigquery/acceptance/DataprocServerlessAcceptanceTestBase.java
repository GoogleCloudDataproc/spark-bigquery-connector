/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.acceptance;

import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.CONNECTOR_JAR_DIRECTORY;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.createBqDataset;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.deleteBqDatasetAndTables;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.generateClusterName;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.uploadConnectorJar;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.BatchControllerClient;
import com.google.cloud.dataproc.v1.BatchControllerSettings;
import com.google.cloud.dataproc.v1.BatchOperationMetadata;
import com.google.cloud.dataproc.v1.CreateBatchRequest;
import com.google.cloud.dataproc.v1.EnvironmentConfig;
import com.google.cloud.dataproc.v1.ExecutionConfig;
import com.google.cloud.dataproc.v1.PySparkBatch;
import com.google.cloud.dataproc.v1.RuntimeConfig;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;

public class DataprocServerlessAcceptanceTestBase {

  BatchControllerClient batchController;
  String testName =
      getClass()
          .getSimpleName()
          .substring(0, getClass().getSimpleName().length() - 32)
          .toLowerCase(Locale.ENGLISH);
  String testId = String.format("%s-%s", testName, System.currentTimeMillis());
  String testBaseGcsDir = AcceptanceTestUtils.createTestBaseGcsDir(testId);
  String connectorJarUri = testBaseGcsDir + "/connector.jar";
  AcceptanceTestContext context =
      new AcceptanceTestContext(
          testId, generateClusterName(testId), testBaseGcsDir, connectorJarUri);

  private final String connectorJarPrefix;
  private final String s8sImageVersion;

  public DataprocServerlessAcceptanceTestBase(String connectorJarPrefix, String s8sImageVersion) {
    this.connectorJarPrefix = connectorJarPrefix;
    this.s8sImageVersion = s8sImageVersion;
  }

  @Before
  public void createBatchControllerClient() throws Exception {
    uploadConnectorJar(CONNECTOR_JAR_DIRECTORY, connectorJarPrefix, context.connectorJarUri);
    createBqDataset(context.bqDataset);

    batchController =
        BatchControllerClient.create(
            BatchControllerSettings.newBuilder()
                .setEndpoint(AcceptanceTestConstants.DATAPROC_ENDPOINT)
                .build());
  }

  @After
  public void tearDown() throws Exception {
    batchController.close();
    AcceptanceTestUtils.deleteGcsDir(context.testBaseGcsDir);
    deleteBqDatasetAndTables(context.bqDataset);
  }

  protected OperationSnapshot createAndRunPythonBatch(
      AcceptanceTestContext context,
      String testName,
      String pythonFile,
      String pythonZipUri,
      List<String> args)
      throws Exception {
    AcceptanceTestUtils.uploadToGcs(
        DataprocServerlessAcceptanceTestBase.class.getResourceAsStream("/acceptance/" + pythonFile),
        context.getScriptUri(testName),
        "text/x-python");
    String parent =
        String.format(
            "projects/%s/locations/%s",
            AcceptanceTestConstants.PROJECT_ID, AcceptanceTestConstants.REGION);
    Batch batch =
        Batch.newBuilder()
            .setName(parent + "/batches/" + context.clusterId)
            .setPysparkBatch(createPySparkBatchBuilder(context, testName, pythonZipUri, args))
            .setRuntimeConfig(RuntimeConfig.newBuilder().setVersion(s8sImageVersion))
            .setEnvironmentConfig(
                EnvironmentConfig.newBuilder()
                    .setExecutionConfig(
                        ExecutionConfig.newBuilder()
                            .setNetworkUri(AcceptanceTestConstants.SERVERLESS_NETWORK_URI)))
            .build();

    OperationFuture<Batch, BatchOperationMetadata> batchAsync =
        batchController.createBatchAsync(
            CreateBatchRequest.newBuilder()
                .setParent(parent)
                .setBatchId(context.clusterId)
                .setBatch(batch)
                .build());

    return batchAsync
        .getPollingFuture()
        .get(AcceptanceTestConstants.SERVERLESS_BATCH_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
  }

  protected PySparkBatch.Builder createPySparkBatchBuilder(
      AcceptanceTestContext context, String testName, String pythonZipUri, List<String> args) {
    PySparkBatch.Builder builder =
        PySparkBatch.newBuilder()
            .setMainPythonFileUri(context.getScriptUri(testName))
            .addJarFileUris(context.connectorJarUri);

    if (pythonZipUri != null && pythonZipUri.length() != 0) {
      builder.addPythonFileUris(pythonZipUri);
      builder.addFileUris(pythonZipUri);
    }

    if (args != null && args.size() != 0) {
      builder.addAllArgs(args);
    }

    return builder;
  }
}
