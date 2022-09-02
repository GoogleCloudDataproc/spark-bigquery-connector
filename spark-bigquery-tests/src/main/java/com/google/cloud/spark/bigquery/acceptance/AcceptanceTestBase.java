/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.PROJECT_ID;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestConstants.REGION;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.createBqDataset;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.deleteBqDatasetAndTables;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.generateClusterName;
import static com.google.cloud.spark.bigquery.acceptance.AcceptanceTestUtils.uploadConnectorJar;

import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterConfig;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.dataproc.v1.DiskConfig;
import com.google.cloud.dataproc.v1.GceClusterConfig;
import com.google.cloud.dataproc.v1.InstanceGroupConfig;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobPlacement;
import com.google.cloud.dataproc.v1.PySparkJob;
import com.google.cloud.dataproc.v1.SoftwareConfig;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AcceptanceTestBase {

  protected static final DataprocAcceptanceTestBase.ClusterProperty DISABLE_CONSCRYPT =
      DataprocAcceptanceTestBase.ClusterProperty.of(
          "dataproc:dataproc.conscrypt.provider.enable", "false", "nc");
  protected static final ImmutableList<DataprocAcceptanceTestBase.ClusterProperty>
      DISABLE_CONSCRYPT_LIST =
          ImmutableList.<DataprocAcceptanceTestBase.ClusterProperty>builder()
              .add(DISABLE_CONSCRYPT)
              .build();

  protected AcceptanceTestContext context;
  protected boolean sparkStreamingSupported;

  protected AcceptanceTestBase(AcceptanceTestContext context) {
    this(context, true);
  }

  protected AcceptanceTestBase(AcceptanceTestContext context, boolean sparkStreamingSupported) {
    this.context = context;
    this.sparkStreamingSupported = sparkStreamingSupported;
  }

  protected static AcceptanceTestContext setup(
      String dataprocImageVersion,
      String connectorJarPrefix,
      List<DataprocAcceptanceTestBase.ClusterProperty> clusterProperties)
      throws Exception {
    return setup(dataprocImageVersion, connectorJarPrefix, clusterProperties, cluster -> cluster);
  }

  protected static AcceptanceTestContext setup(
      String dataprocImageVersion,
      String connectorJarPrefix,
      List<DataprocAcceptanceTestBase.ClusterProperty> clusterProperties,
      ClusterCustomizer clusterCustomizer)
      throws Exception {
    String clusterPropertiesMarkers =
        clusterProperties.isEmpty()
            ? ""
            : clusterProperties.stream()
                .map(DataprocAcceptanceTestBase.ClusterProperty::getMarker)
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
            .collect(
                Collectors.toMap(
                    DataprocAcceptanceTestBase.ClusterProperty::getKey,
                    DataprocAcceptanceTestBase.ClusterProperty::getValue));
    String clusterName =
        createClusterIfNeeded(dataprocImageVersion, testId, properties, clusterCustomizer);
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
      String dataprocImageVersion,
      String testId,
      Map<String, String> properties,
      final ClusterCustomizer clusterCustomizer)
      throws Exception {
    String clusterName = generateClusterName(testId);
    Cluster clusterDefinition =
        clusterCustomizer.customize(createCluster(clusterName, dataprocImageVersion, properties));
    cluster(client -> client.createClusterAsync(PROJECT_ID, REGION, clusterDefinition).get());
    return clusterDefinition.getClusterName();
  }

  protected static void terminateCluster(String clusterName) throws Exception {
    cluster(client -> client.deleteClusterAsync(PROJECT_ID, REGION, clusterName).get());
  }

  protected static void cluster(
      DataprocAcceptanceTestBase.ThrowingConsumer<ClusterControllerClient> command)
      throws Exception {
    try (ClusterControllerClient clusterControllerClient =
        ClusterControllerClient.create(
            ClusterControllerSettings.newBuilder().setEndpoint(DATAPROC_ENDPOINT).build())) {
      command.accept(clusterControllerClient);
    }
  }

  protected static Cluster createCluster(
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

  protected Job createAndRunPythonJob(
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

  protected PySparkJob.Builder createPySparkJobBuilder(
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

  protected Job runAndWait(Job job, Duration timeout) throws Exception {
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
  protected interface ThrowingConsumer<T> {
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
