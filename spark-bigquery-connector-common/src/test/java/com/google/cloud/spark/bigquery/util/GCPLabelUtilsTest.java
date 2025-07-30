package com.google.cloud.spark.bigquery.util;

import static org.junit.Assert.assertEquals;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;

public class GCPLabelUtilsTest {
  private ClientAndServer mockServer;
  private String mockBaseUrl;

  public static final Header METADATA_HEADER = new Header("Metadata-Flavor", "Google");
  private static final String TEST_APP_NAME = "labels-test";
  private static final String TEST_APP_ID = "application_12345";
  private static final String TEST_RESOURCE_UUID = "1q2w3e4r5t6y7u8i";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_JOB_ID = "test-job";
  private static final String TEST_BATCH_ID = "test-batch";
  private static final String TEST_SESSION_ID = "test-session";
  private static final String TEST_PROJECT_ID = "test-project";
  private static final String TEST_REGION = "us-central1";

  private static final Map<String, Object> EXPECTED_FACET_DATAPROC_CLUSTER = new HashMap<>();
  private static final Map<String, Object> EXPECTED_FACET_DATAPROC_BATCH = new HashMap<>();
  private static final Map<String, Object> EXPECTED_FACET_DATAPROC_SESSION = new HashMap<>();

  @Before
  public void setUp() {
    mockServer = ClientAndServer.startClientAndServer();
    mockBaseUrl = "http://localhost:" + mockServer.getPort();
  }

  @After
  public void tearDown() {
    mockServer.stop();
  }

  @Test
  public void testGetSparkLabelsOnlyAppInfo() {
    ImmutableMap<String, String> conf =
        ImmutableMap.of(
            GCPLabelUtils.SPARK_APP_ID, TEST_APP_ID,
            GCPLabelUtils.SPARK_APP_NAME, TEST_APP_NAME);

    Map<String, String> labels = GCPLabelUtils.getSparkLabels(conf);

    assertEquals(2, labels.size());
    assertEquals(TEST_APP_ID, labels.get("appId"));
    assertEquals(TEST_APP_NAME, labels.get("appName"));
  }

  @Test
  public void testGetGCPLabelsClusterMode() {
    // Setup mock server responses for cluster mode
    setupMockServerForCluster();

    ImmutableMap<String, String> conf =
        ImmutableMap.<String, String>builder()
            .put(GCPLabelUtils.SPARK_MASTER, "yarn")
            .put(
                GCPLabelUtils.SPARK_DRIVER_HOST,
                TEST_CLUSTER_NAME + "-m.us-central1-a.c.hadoop-cloud-dev.google.com.internal")
            .put(
                GCPLabelUtils.SPARK_YARN_TAGS,
                "dataproc_job_" + TEST_JOB_ID + ",dataproc_uuid_" + TEST_RESOURCE_UUID)
            .put(GCPLabelUtils.SPARK_APP_ID, TEST_APP_ID)
            .put(GCPLabelUtils.SPARK_APP_NAME, TEST_APP_NAME)
            .put(GCPLabelUtils.GOOGLE_METADATA_API, mockBaseUrl)
            .build();

    Map<String, String> labels = GCPLabelUtils.getGCPLabels(conf);

    // Verify all expected labels are present with correct values
    assertResults(labels, EXPECTED_FACET_DATAPROC_CLUSTER);
  }

  @Test
  public void testGetGCPLabelsBatchMode() {
    // Setup mock server responses for batch mode
    setupMockServerForBatch();

    ImmutableMap<String, String> conf =
        ImmutableMap.<String, String>builder()
            .put(GCPLabelUtils.SPARK_APP_ID, TEST_APP_ID)
            .put(GCPLabelUtils.SPARK_APP_NAME, TEST_APP_NAME)
            .put(GCPLabelUtils.GOOGLE_METADATA_API, mockBaseUrl)
            .build();

    Map<String, String> labels = GCPLabelUtils.getGCPLabels(conf);

    // Verify all expected labels are present with correct values
    assertResults(labels, EXPECTED_FACET_DATAPROC_BATCH);
  }

  @Test
  public void testGetGCPLabelsSessionMode() {
    // Setup mock server responses for session mode
    setupMockServerForSession();

    ImmutableMap<String, String> conf =
        ImmutableMap.<String, String>builder()
            .put(GCPLabelUtils.SPARK_APP_ID, TEST_APP_ID)
            .put(GCPLabelUtils.SPARK_APP_NAME, TEST_APP_NAME)
            .put(GCPLabelUtils.GOOGLE_METADATA_API, mockBaseUrl)
            .build();

    Map<String, String> labels = GCPLabelUtils.getGCPLabels(conf);

    // Verify all expected labels are present with correct values
    assertResults(labels, EXPECTED_FACET_DATAPROC_SESSION);
  }

  private static void assertResults(
      Map<String, String> labels, Map<String, Object> expectedFacetDataprocCluster) {
    for (Map.Entry<String, Object> expected : expectedFacetDataprocCluster.entrySet()) {
      assertEquals(
          "Missing or incorrect value for key: " + expected.getKey(),
          expected.getValue().toString(),
          labels.get(expected.getKey()));
    }
  }

  private void setupMockServerForCluster() {
    setupMockServerBaseSetup();
    return200ForEndpoint(GCPLabelUtils.CLUSTER_UUID_ENDPOINT, TEST_RESOURCE_UUID);
  }

  private void setupMockServerForBatch() {
    setupMockServerBaseSetup();
    return200ForEndpoint(GCPLabelUtils.BATCH_ID_ENDPOINT, TEST_BATCH_ID);
    return200ForEndpoint(GCPLabelUtils.BATCH_UUID_ENDPOINT, TEST_RESOURCE_UUID);
    return404ForEndpoint(GCPLabelUtils.SESSION_ID_ENDPOINT);
    return404ForEndpoint(GCPLabelUtils.SESSION_UUID_ENDPOINT);
  }

  private void setupMockServerForSession() {
    setupMockServerBaseSetup();
    return200ForEndpoint(GCPLabelUtils.SESSION_ID_ENDPOINT, TEST_SESSION_ID);
    return200ForEndpoint(GCPLabelUtils.SESSION_UUID_ENDPOINT, TEST_RESOURCE_UUID);
    return200ForEndpoint(GCPLabelUtils.PROJECT_ID_ENDPOINT, "projects/456/" + TEST_PROJECT_ID);
    return200ForEndpoint(GCPLabelUtils.DATAPROC_REGION_ENDPOINT, TEST_REGION);
    return404ForEndpoint(GCPLabelUtils.BATCH_ID_ENDPOINT);
    return404ForEndpoint(GCPLabelUtils.BATCH_UUID_ENDPOINT);
  }

  private void setupMockServerBaseSetup() {
    return200ForEndpoint(GCPLabelUtils.PROJECT_ID_ENDPOINT, "projects/123456/" + TEST_PROJECT_ID);
    return200ForEndpoint(GCPLabelUtils.DATAPROC_REGION_ENDPOINT, TEST_REGION);
  }

  private void return200ForEndpoint(String endpoint, String responseBody) {
    mockServer
        .when(request().withMethod("GET").withPath(endpoint).withHeader(METADATA_HEADER))
        .respond(response().withBody(responseBody));
  }

  private void return404ForEndpoint(String endpoint) {
    mockServer
        .when(request().withMethod("GET").withPath(endpoint))
        .respond(response().withStatusCode(404));
  }

  static {
    EXPECTED_FACET_DATAPROC_CLUSTER.put("job.uuid", TEST_RESOURCE_UUID);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("job.id", TEST_JOB_ID);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("cluster.uuid", TEST_RESOURCE_UUID);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("cluster.name", TEST_CLUSTER_NAME);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("projectId", TEST_PROJECT_ID);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("job.type", "dataproc_job");
    EXPECTED_FACET_DATAPROC_CLUSTER.put("region", TEST_REGION);

    EXPECTED_FACET_DATAPROC_BATCH.put("spark.batch.uuid", TEST_RESOURCE_UUID);
    EXPECTED_FACET_DATAPROC_BATCH.put("spark.batch.id", TEST_BATCH_ID);
    EXPECTED_FACET_DATAPROC_BATCH.put("projectId", TEST_PROJECT_ID);
    EXPECTED_FACET_DATAPROC_BATCH.put("job.type", "batch");
    EXPECTED_FACET_DATAPROC_BATCH.put("region", TEST_REGION);

    EXPECTED_FACET_DATAPROC_SESSION.put("spark.session.uuid", TEST_RESOURCE_UUID);
    EXPECTED_FACET_DATAPROC_SESSION.put("spark.session.id", TEST_SESSION_ID);
    EXPECTED_FACET_DATAPROC_SESSION.put("projectId", TEST_PROJECT_ID);
    EXPECTED_FACET_DATAPROC_SESSION.put("job.type", "session");
    EXPECTED_FACET_DATAPROC_SESSION.put("region", TEST_REGION);
  }
}
