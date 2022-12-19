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

import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.CONNECTOR_JAR_DIRECTORY;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.DATAPROC_ENDPOINT;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.MAX_BIG_NUMERIC;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.MIN_BIG_NUMERIC;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.PROJECT_ID;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.REGION;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.createBqDataset;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.deleteBqDatasetAndTables;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.generateClusterName;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.getNumOfRowsOfBqTable;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.runBqQuery;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.uploadConnectorJar;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.dataproc.v1.*;
import com.google.common.collect.ImmutableList;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Test;

public class DataprocAcceptanceTestBase {

  protected static final ClusterProperty DISABLE_CONSCRYPT =
      ClusterProperty.of("dataproc:dataproc.conscrypt.provider.enable", "false", "nc");
  protected static final ImmutableList<ClusterProperty> DISABLE_CONSCRYPT_LIST =
      ImmutableList.<ClusterProperty>builder().add(DISABLE_CONSCRYPT).build();

  private AcceptanceTestContext context;
  private boolean sparkStreamingSupported;

  protected DataprocAcceptanceTestBase(AcceptanceTestContext context) {
    this(context, true);
  }

  protected DataprocAcceptanceTestBase(
      AcceptanceTestContext context, boolean sparkStreamingSupported) {
    this.context = context;
    this.sparkStreamingSupported = sparkStreamingSupported;
  }

  protected static AcceptanceTestContext setup(
      String dataprocImageVersion,
      String connectorJarPrefix,
      List<ClusterProperty> clusterProperties)
      throws Exception {
    String clusterPropertiesMarkers =
        clusterProperties.isEmpty()
            ? ""
            : clusterProperties.stream()
                .map(ClusterProperty::getMarker)
                .collect(Collectors.joining("-", "-", ""));
    String testId =
        String.format(
            "%s-%s%s%s",
            System.currentTimeMillis(),
            dataprocImageVersion.charAt(0),
            dataprocImageVersion.charAt(2),
            clusterPropertiesMarkers);
    Map<String, String> properties =
        clusterProperties.stream()
            .collect(Collectors.toMap(ClusterProperty::getKey, ClusterProperty::getValue));
    String clusterName = createClusterIfNeeded(dataprocImageVersion, testId, properties);
    AcceptanceTestContext acceptanceTestContext = new AcceptanceTestContext(testId, clusterName);
    uploadConnectorJar(
        CONNECTOR_JAR_DIRECTORY, connectorJarPrefix, acceptanceTestContext.connectorJarUri);
    createBqDataset(acceptanceTestContext.bqDataset);
    return acceptanceTestContext;
  }

  protected static void tearDown(AcceptanceTestContext context) throws Exception {
    if (context != null) {
      terminateCluster(context.clusterId);
      AcceptanceTestUtils.deleteGcsDir(context.testBaseGcsDir);
      deleteBqDatasetAndTables(context.bqDataset);
    }
  }

  protected static String createClusterIfNeeded(
      String dataprocImageVersion, String testId, Map<String, String> properties) throws Exception {
    String clusterName = generateClusterName(testId);
    cluster(
        client ->
            client
                .createClusterAsync(
                    PROJECT_ID,
                    REGION,
                    createCluster(clusterName, dataprocImageVersion, properties))
                .get());
    return clusterName;
  }

  protected static void terminateCluster(String clusterName) throws Exception {
    cluster(client -> client.deleteClusterAsync(PROJECT_ID, REGION, clusterName).get());
  }

  private static void cluster(ThrowingConsumer<ClusterControllerClient> command) throws Exception {
    try (ClusterControllerClient clusterControllerClient =
        ClusterControllerClient.create(
            ClusterControllerSettings.newBuilder().setEndpoint(DATAPROC_ENDPOINT).build())) {
      command.accept(clusterControllerClient);
    }
  }

  private static Cluster createCluster(
      String clusterName, String dataprocImageVersion, Map<String, String> properties) {
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
                    SoftwareConfig.newBuilder()
                        .setImageVersion(dataprocImageVersion)
                        .putAllProperties(properties)))
        .build();
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

  private Job createAndRunPythonJob(
      String testName, String pythonFile, String pythonZipUri, List<String> args, long duration)
      throws Exception {
    AcceptanceTestUtils.uploadToGcs(
        getClass().getResourceAsStream("/acceptance/" + pythonFile),
        context.getScriptUri(testName),
        "text/x-python");

    Job job =
        Job.newBuilder()
            .setPlacement(JobPlacement.newBuilder().setClusterName(context.clusterId))
            .setPysparkJob(createPySparkJobBuilder(testName, pythonZipUri, args))
            .build();

    return runAndWait(job, Duration.ofSeconds(duration));
  }

  private PySparkJob.Builder createPySparkJobBuilder(
      String testName, String pythonZipUri, List<String> args) {
    PySparkJob.Builder builder =
        PySparkJob.newBuilder()
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

  private Job runAndWait(Job job, Duration timeout) throws Exception {
    try (JobControllerClient jobControllerClient =
        JobControllerClient.create(
            JobControllerSettings.newBuilder().setEndpoint(DATAPROC_ENDPOINT).build())) {
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

  protected static class ClusterProperty {
    private String key;
    private String value;
    private String marker;

    private ClusterProperty(String key, String value, String marker) {
      this.key = key;
      this.value = value;
      this.marker = marker;
    }

    protected static ClusterProperty of(String key, String value, String marker) {
      return new ClusterProperty(key, value, marker);
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    public String getMarker() {
      return marker;
    }
  }
}
