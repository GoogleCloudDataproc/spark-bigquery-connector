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
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Table;
import com.google.cloud.dataproc.v1.*;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Ignore;
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
    String testBaseGcsDir = AcceptanceTestUtils.createTestBaseGcsDir(testId);
    String connectorJarUri = testBaseGcsDir + "/connector.jar";
    uploadConnectorJar(CONNECTOR_JAR_DIRECTORY, connectorJarPrefix, connectorJarUri);

    String clusterName =
        createClusterIfNeeded(dataprocImageVersion, testId, properties, connectorJarUri);
    AcceptanceTestContext acceptanceTestContext =
        new AcceptanceTestContext(testId, clusterName, testBaseGcsDir, connectorJarUri);
    createBqDataset(acceptanceTestContext.bqDataset);
    return acceptanceTestContext;
  }

  @Before
  public void refreshTestTableNames() {
    context.refreshTableNames();
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
      String connectorJarUri)
      throws Exception {
    String clusterName = generateClusterName(testId);
    cluster(
        client ->
            client
                .createClusterAsync(
                    PROJECT_ID,
                    REGION,
                    createCluster(clusterName, dataprocImageVersion, properties, connectorJarUri))
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
      String clusterName,
      String dataprocImageVersion,
      Map<String, String> properties,
      String connectorJarUri) {
    return Cluster.newBuilder()
        .setClusterName(clusterName)
        .setProjectId(PROJECT_ID)
        .setConfig(
            ClusterConfig.newBuilder()
                .setGceClusterConfig(
                    GceClusterConfig.newBuilder()
                        .setNetworkUri("default")
                        .setInternalIpOnly(false)
                        .setZoneUri(REGION + "-a")
                        .putMetadata("SPARK_BQ_CONNECTOR_URL", connectorJarUri))
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
                .setEndpointConfig(
                    EndpointConfig.newBuilder().setEnableHttpPortAccess(true).build())
                .setSoftwareConfig(
                    SoftwareConfig.newBuilder()
                        .setImageVersion(dataprocImageVersion)
                        .putAllProperties(properties)))
        .build();
  }

  @Test
  @Ignore
  public void testRead() throws Exception {
    String testName = "test-read";
    Job result =
        createAndRunPythonJob(
            testName,
            "read_shakespeare.py",
            null,
            Arrays.asList(context.getResultsDirUri(testName)),
            120);

    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo("spark,10");
  }

  @Test
  @Ignore
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

    int numOfRows = getNumOfRowsOfBqTable(context.bqDataset, context.bqStreamTable);
    assertThat(numOfRows == 2);
  }

  @Test
  @Ignore
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

    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo(MIN_BIG_NUMERIC + "," + MAX_BIG_NUMERIC);
  }

  @Test
  public void testSparkDml_explicitTableCreation() throws Exception {
    String testName = "spark-dml-explicit-table";

    Job result = createAndRunSparkSqlJob(testName, /* outputTable */ "N/A", /* duration */ 120);
    String output = AcceptanceTestUtils.getDriverOutput(result);
    assertThat(output.trim()).contains("spark\t10");
  }

  @Test
  public void testSparkDml_createTableInDefaultDataset() throws Exception {
    String testName = "spark-dml-create-table-in-default-dataset";

    testWithTableInDefaultDataset(testName);
  }

  @Test
  public void testSparkDml_createTableAsSelectInDefaultDataset() throws Exception {
    String testName = "spark-dml-create-table-as-select-in-default-dataset";

    testWithTableInDefaultDataset(testName);
  }

  private void testWithTableInDefaultDataset(String testName) throws Exception {
    BigQuery bq = BigQueryOptions.getDefaultInstance().getService();
    Dataset defaultDataset = bq.getDataset("default");
    assertWithMessage("This test assume that the a dataset named `default` exists in the project")
        .that(defaultDataset)
        .isNotNull();

    Job result = createAndRunSparkSqlJob(testName, /* outputTable */ "N/A", /* duration */ 120);

    Table table = null;
    try {
      table = bq.getTable("default", context.bqTable);
      assertThat(table).isNotNull();
      assertThat(table.getNumRows().intValue()).isEqualTo(1);
    } finally {
      if (table != null) {
        assertThat(table.delete()).isTrue();
      }
    }
  }

  @Test
  public void testSparkDml_createTableInCustomDataset() throws Exception {
    String testName = "spark-dml-custom-dataset";

    BigQuery bq = BigQueryOptions.getDefaultInstance().getService();
    Dataset defaultDataset = bq.getDataset(context.bqDataset);
    assertWithMessage(
            "This test assume that the a dataset named `"
                + context.bqDataset
                + "` exists in the project")
        .that(defaultDataset)
        .isNotNull();

    Job result = createAndRunSparkSqlJob(testName, /* outputTable */ "N/A", /* duration */ 120);

    Table table = null;
    try {
      table = bq.getTable("default", context.bqTable);
      assertThat(table).isNotNull();
      assertThat(table.getNumRows().intValue()).isEqualTo(1);
    } finally {
      if (table != null) {
        assertThat(table.delete()).isTrue();
      }
    }
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
            .putLabels("test-name", testName)
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

    if (pythonZipUri != null && !pythonZipUri.isEmpty()) {
      builder.addPythonFileUris(pythonZipUri);
      builder.addFileUris(pythonZipUri);
    }

    if (args != null && !args.isEmpty()) {
      builder.addAllArgs(args);
    }

    return builder;
  }

  private Job createAndRunSparkSqlJob(String testName, String outputTable, long duration)
      throws Exception {

    InputStream sqlScriptSourceStream =
        getClass().getResourceAsStream("/acceptance/" + testName + ".sql");
    assertWithMessage("Could not find SQL script for " + testName + ".sql")
        .that(sqlScriptSourceStream)
        .isNotNull();
    String sqlScriptSource =
        new String(ByteStreams.toByteArray(sqlScriptSourceStream), StandardCharsets.UTF_8);

    String sqlScript =
        sqlScriptSource
            .replace("${DATASET}", context.bqDataset)
            .replace("${TABLE}", context.bqTable)
            .replace("${OUTPUT_TABLE}", outputTable);

    AcceptanceTestUtils.uploadToGcs(
        new ByteArrayInputStream(sqlScript.getBytes(StandardCharsets.UTF_8)),
        context.getScriptUri(testName),
        "application/x-sql");

    Job job =
        Job.newBuilder()
            .setPlacement(JobPlacement.newBuilder().setClusterName(context.clusterId))
            .putLabels("test-name", testName)
            .setSparkSqlJob(createSparkSqlJobBuilder(testName))
            .build();

    return runAndWait(job, Duration.ofSeconds(duration));
  }

  private SparkSqlJob.Builder createSparkSqlJobBuilder(String testName) {
    return SparkSqlJob.newBuilder()
        .setQueryFileUri(context.getScriptUri(testName))
        .addJarFileUris(context.connectorJarUri)
        .setLoggingConfig(
            LoggingConfig.newBuilder()
                .putDriverLogLevels("com", LoggingConfig.Level.DEBUG)
                .putDriverLogLevels("io", LoggingConfig.Level.DEBUG)
                .putDriverLogLevels("org", LoggingConfig.Level.DEBUG)
                .putDriverLogLevels("net", LoggingConfig.Level.DEBUG)
                .build())
        .putProperties("spark.sql.legacy.createHiveTableByDefault", "false")
        .putProperties("spark.sql.sources.default", "bigquery")
        .putProperties("spark.datasource.bigquery.writeMethod", "direct");
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
      assertThat(jobInfo.getStatus().getState()).isEqualTo(JobStatus.State.DONE);
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
