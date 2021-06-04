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
package com.google.spark.bigquery.acceptance;

import com.google.cloud.dataproc.v1.*;
import java.io.FileInputStream;
import org.junit.AssumptionViolatedException;
import org.junit.Test;
import scala.util.Properties;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.spark.bigquery.acceptance.AcceptanceTestConstants.MAX_BIG_NUMERIC;
import static com.google.spark.bigquery.acceptance.AcceptanceTestConstants.MIN_BIG_NUMERIC;
import static com.google.spark.bigquery.acceptance.AcceptanceTestUtils.createBqDataset;
import static com.google.spark.bigquery.acceptance.AcceptanceTestUtils.createZipFile;
import static com.google.spark.bigquery.acceptance.AcceptanceTestUtils.deleteBqDatasetAndTables;
import static com.google.spark.bigquery.acceptance.AcceptanceTestUtils.runBqQuery;

public class DataprocAcceptanceTestBase {

  public static final String US_CENTRAL_1_DATAPROC_ENDPOINT =
      "us-central1-dataproc.googleapis.com:443";
  private static final String PROJECT_ID = System.getenv("GOOGLE_CLOUD_PROJECT");
  private static final String REGION = "us-central1";
  private AcceptanceTestContext context;

  protected DataprocAcceptanceTestBase(AcceptanceTestContext context) {
    this.context = context;
  }

  protected static AcceptanceTestContext setup(String scalaVersion, String dataprocImageVersion)
      throws Exception {
    // this line will abort the test for the wrong scala version
    String runtimeScalaVersion = Properties.versionNumberString();
    if (!runtimeScalaVersion.startsWith(scalaVersion)) {
      throw new AssumptionViolatedException(
          String.format(
              "Test is for scala %s, Runtime is scala %s", scalaVersion, runtimeScalaVersion));
    }
    String testId =
        String.format(
            "%s-%s%s",
            System.currentTimeMillis(),
            dataprocImageVersion.charAt(0),
            dataprocImageVersion.charAt(2));
    String clusterName = createClusterIfNeeded(dataprocImageVersion, testId);
    AcceptanceTestContext acceptanceTestContext = new AcceptanceTestContext(testId, clusterName);
    uploadConnectorJar(scalaVersion, acceptanceTestContext.connectorJarUri);
    return acceptanceTestContext;
  }

  protected static void tearDown(AcceptanceTestContext context) throws Exception {
    if (context != null) {
      terminateCluster(context.clusterId);
      AcceptanceTestUtils.deleteGcsDir(context.testBaseGcsDir);
      deleteBqDatasetAndTables(context.testBigNumericDataset);
    }
  }

  protected static String createClusterIfNeeded(String dataprocImageVersion, String testId)
      throws Exception {
    String clusterName = generateClusterName(dataprocImageVersion, testId);
    cluster(
        client ->
            client
                .createClusterAsync(
                    PROJECT_ID, REGION, createCluster(clusterName, dataprocImageVersion))
                .get());
    return clusterName;
  }

  protected static void terminateCluster(String clusterName) throws Exception {
    cluster(client -> client.deleteClusterAsync(PROJECT_ID, REGION, clusterName).get());
  }

  private static void cluster(ThrowingConsumer<ClusterControllerClient> command) throws Exception {
    try (ClusterControllerClient clusterControllerClient =
        ClusterControllerClient.create(
            ClusterControllerSettings.newBuilder()
                .setEndpoint("us-central1-dataproc.googleapis.com:443")
                .build())) {
      command.accept(clusterControllerClient);
    }
  }

  private static String generateClusterName(String dataprocImageVersion, String testId) {
    return String.format("spark-bigquery-it-%s", testId);
  }

  private static Cluster createCluster(String clusterName, String dataprocImageVersion) {
    return Cluster.newBuilder()
        .setClusterName(clusterName)
        .setProjectId(PROJECT_ID)
        .setConfig(
            ClusterConfig.newBuilder()
                .setGceClusterConfig(
                    GceClusterConfig.newBuilder()
                        .setNetworkUri("default")
                        .setZoneUri(REGION + "-a"))
                .setMasterConfig(
                    InstanceGroupConfig.newBuilder()
                        .setNumInstances(1)
                        .setMachineTypeUri("n1-standard-4")
                        .setDiskConfig(
                            DiskConfig.newBuilder()
                                .setBootDiskType("pd-standard")
                                .setBootDiskSizeGb(300)
                                .setNumLocalSsds(0)))
                .setWorkerConfig(
                    InstanceGroupConfig.newBuilder()
                        .setNumInstances(2)
                        .setMachineTypeUri("n1-standard-4")
                        .setDiskConfig(
                            DiskConfig.newBuilder()
                                .setBootDiskType("pd-standard")
                                .setBootDiskSizeGb(300)
                                .setNumLocalSsds(0)))
                .setSoftwareConfig(
                    SoftwareConfig.newBuilder().setImageVersion(dataprocImageVersion)))
        .build();
  }

  private static void uploadConnectorJar(String scalaVersion, String connectorJarUri)
      throws Exception {
    Path targetDir = Paths.get(String.format("fatJar/target/scala-%s/", scalaVersion));
    Path assemblyJar = AcceptanceTestUtils.getAssemblyJar(targetDir);
    AcceptanceTestUtils.copyToGcs(assemblyJar, connectorJarUri, "application/java-archive");
  }

  @Test
  public void testRead() throws Exception {
    String testName = "test-read";
    AcceptanceTestUtils.uploadToGcs(
        getClass().getResourceAsStream("/acceptance/read_shakespeare.py"),
        context.getScriptUri(testName),
        "text/x-python");
    Job job =
        Job.newBuilder()
            .setPlacement(JobPlacement.newBuilder().setClusterName(context.clusterId))
            .setPysparkJob(
                PySparkJob.newBuilder()
                    .setMainPythonFileUri(context.getScriptUri(testName))
                    .addJarFileUris(context.connectorJarUri)
                    .addArgs(context.getResultsDirUri(testName)))
            .build();
    Job result = runAndWait(job, Duration.ofSeconds(60));
    assertThat(result.getStatus().getState()).isEqualTo(JobStatus.State.DONE);
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo("spark,10");
  }

  @Test
  public void testBigNumeric() throws Exception {
    String testName = "test-big-numeric";
    String pyBaseDir = Paths.get("pythonlib").toAbsolutePath().toString();
    String zipFileLocation = pyBaseDir + "/big_numeric_acceptance_test.zip";
    String zipFileUri =
        context.testBaseGcsDir + "/" + testName + "/big_numeric_acceptance_test.zip";

    createZipFile(pyBaseDir, zipFileLocation);

    AcceptanceTestUtils.uploadToGcs(
        getClass().getResourceAsStream("/acceptance/big_numeric.py"),
        context.getScriptUri(testName),
        "text/x-python");
    AcceptanceTestUtils.uploadToGcs(
        new FileInputStream(zipFileLocation), zipFileUri, "application/zip");

    createBqDataset(context.testBigNumericDataset);

    runBqQuery(
        String.format(
            AcceptanceTestConstants.BIGNUMERIC_TABLE_QUERY_TEMPLATE,
            context.testBigNumericDataset,
            context.testBigNumericTable));

    Job job =
        Job.newBuilder()
            .setPlacement(JobPlacement.newBuilder().setClusterName(context.clusterId))
            .setPysparkJob(
                PySparkJob.newBuilder()
                    .setMainPythonFileUri(context.getScriptUri(testName))
                    .addJarFileUris(context.connectorJarUri)
                    .addPythonFileUris(zipFileUri)
                    .addFileUris(zipFileUri)
                    .addArgs(context.testBigNumericDataset + "." + context.testBigNumericTable)
                    .addArgs(context.getResultsDirUri(testName)))
            .build();

    Job result = runAndWait(job, Duration.ofSeconds(60));
    assertThat(result.getStatus().getState()).isEqualTo(JobStatus.State.DONE);
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo(MIN_BIG_NUMERIC + "," + MAX_BIG_NUMERIC);
  }

  private Job runAndWait(Job job, Duration timeout) throws Exception {
    try (JobControllerClient jobControllerClient =
        JobControllerClient.create(
            JobControllerSettings.newBuilder()
                .setEndpoint(US_CENTRAL_1_DATAPROC_ENDPOINT)
                .build())) {
      Job request = jobControllerClient.submitJob(PROJECT_ID, REGION, job);
      String jobId = request.getReference().getJobId();
      CompletableFuture<Job> finishedJobFuture =
          CompletableFuture.supplyAsync(
              () -> waitForJobCompletion(jobControllerClient, PROJECT_ID, REGION, jobId));
      Job jobInfo = finishedJobFuture.get(timeout.getSeconds(), TimeUnit.SECONDS);
      return jobInfo;
    }
  }

  Job waitForJobCompletion(
      JobControllerClient jobControllerClient, String projectId, String region, String jobId) {
    while (true) {
      // Poll the service periodically until the Job is in a finished state.
      Job jobInfo = jobControllerClient.getJob(projectId, region, jobId);
      switch (jobInfo.getStatus().getState()) {
        case DONE:
        case CANCELLED:
        case ERROR:
          return jobInfo;
        default:
          try {
            // Wait a second in between polling attempts.
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
      }
    }
  }

  @FunctionalInterface
  private interface ThrowingConsumer<T> {
    void accept(T t) throws Exception;
  }
}
