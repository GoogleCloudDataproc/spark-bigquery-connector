package com.google.cloud.spark.bigquery.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;

/** Util to extract values from GCP environment */
public class GCPLabelUtils {

  private static final String BASE_URI = "http://metadata.google.internal/computeMetadata/v1";
  public static final String PROJECT_ID_ENDPOINT = "/project/project-id";
  public static final String BATCH_ID_ENDPOINT = "/instance/attributes/dataproc-batch-id";
  public static final String BATCH_UUID_ENDPOINT = "/instance/attributes/dataproc-batch-uuid";
  public static final String SESSION_ID_ENDPOINT = "/instance/attributes/dataproc-session-id";
  public static final String SESSION_UUID_ENDPOINT = "/instance/attributes/dataproc-session-uuid";
  public static final String CLUSTER_UUID_ENDPOINT = "/instance/attributes/dataproc-cluster-uuid";
  public static final String DATAPROC_REGION_ENDPOINT = "/instance/attributes/dataproc-region";
  private static final String DATAPROC_CLASSPATH = "/usr/local/share/google/dataproc/lib";
  private static final CloseableHttpClient HTTP_CLIENT;
  public static final String SPARK_YARN_TAGS = "spark.yarn.tags";
  public static final String SPARK_DRIVER_HOST = "spark.driver.host";
  public static final String SPARK_APP_ID = "spark.app.id";
  public static final String SPARK_APP_NAME = "spark.app.name";
  public static final String GOOGLE_METADATA_API = "google.metadata.api.base-url";
  public static final String SPARK_MASTER = "spark.master";
  private static final String JOB_ID_PREFIX = "dataproc_job_";
  private static final String JOB_UUID_PREFIX = "dataproc_uuid_";
  private static final String METADATA_FLAVOUR = "Metadata-Flavor";
  private static final String GOOGLE = "Google";
  private static final String SPARK_DIST_CLASSPATH = "SPARK_DIST_CLASSPATH";

  enum ResourceType {
    CLUSTER,
    BATCH,
    INTERACTIVE,
    UNKNOWN
  }

  static {
    ConnectionConfig connectionConfig =
        ConnectionConfig.custom()
            .setConnectTimeout(Timeout.ofMilliseconds(100))
            .setSocketTimeout(Timeout.ofMilliseconds(100))
            .build();
    PoolingHttpClientConnectionManager connMan =
        PoolingHttpClientConnectionManagerBuilder.create()
            .setDefaultConnectionConfig(connectionConfig)
            .build();
    RequestConfig config =
        RequestConfig.custom().setConnectionRequestTimeout(Timeout.ofMilliseconds(100)).build();
    HTTP_CLIENT =
        HttpClients.custom().setDefaultRequestConfig(config).setConnectionManager(connMan).build();
  }

  static boolean isDataprocRuntime() {
    String sparkDistClasspath = System.getenv(SPARK_DIST_CLASSPATH);
    return (sparkDistClasspath != null && sparkDistClasspath.contains(DATAPROC_CLASSPATH));
  }

  public static Map<String, String> getSparkLabels(ImmutableMap<String, String> conf) {
    Map<String, String> sparkLabels = new HashMap<>();
    getSparkAppId(conf).ifPresent(p -> sparkLabels.put("appId", p));
    getSparkAppName(conf).ifPresent(p -> sparkLabels.put("appName", p));
    if (isDataprocRuntime()) {
      sparkLabels.putAll(getGCPLabels(conf));
    }
    return sparkLabels;
  }

  static Map<String, String> getGCPLabels(ImmutableMap<String, String> conf) {
    Map<String, String> gcpLabels = new HashMap<>();
    ResourceType resource = identifyResource(conf);
    switch (resource) {
      case CLUSTER:
        getClusterName(conf).ifPresent(p -> gcpLabels.put("cluster.name", p));
        getClusterUUID(conf).ifPresent(p -> gcpLabels.put("cluster.uuid", p));
        getDataprocJobID(conf).ifPresent(p -> gcpLabels.put("job.id", p));
        getDataprocJobUUID(conf).ifPresent(p -> gcpLabels.put("job.uuid", p));
        gcpLabels.put("job.type", "dataproc_job");
        break;
      case BATCH:
        getDataprocBatchID(conf).ifPresent(p -> gcpLabels.put("spark.batch.id", p));
        getDataprocBatchUUID(conf).ifPresent(p -> gcpLabels.put("spark.batch.uuid", p));
        gcpLabels.put("job.type", "batch");
        break;
      case INTERACTIVE:
        getDataprocSessionID(conf).ifPresent(p -> gcpLabels.put("spark.session.id", p));
        getDataprocSessionUUID(conf).ifPresent(p -> gcpLabels.put("spark.session.uuid", p));
        gcpLabels.put("job.type", "session");
        break;
      case UNKNOWN:
        // do nothing
        break;
    }
    getGCPProjectId(conf).ifPresent(p -> gcpLabels.put("projectId", p));
    getDataprocRegion(conf).ifPresent(p -> gcpLabels.put("region", p));
    return gcpLabels;
  }

  static ResourceType identifyResource(ImmutableMap<String, String> conf) {
    if ("yarn".equals(conf.getOrDefault(SPARK_MASTER, ""))) return ResourceType.CLUSTER;
    if (getDataprocBatchID(conf).isPresent()) return ResourceType.BATCH;
    if (getDataprocSessionID(conf).isPresent()) return ResourceType.INTERACTIVE;

    return ResourceType.UNKNOWN;
  }

  private static Optional<String> getDriverHost(ImmutableMap<String, String> conf) {
    return Optional.ofNullable(conf.get(SPARK_DRIVER_HOST));
  }

  /* sample hostname:
   * sample-cluster-m.us-central1-a.c.hadoop-cloud-dev.google.com.internal */
  @VisibleForTesting
  static Optional<String> getClusterName(ImmutableMap<String, String> conf) {
    return getDriverHost(conf)
        .map(host -> host.split("\\.")[0])
        .map(s -> s.substring(0, s.lastIndexOf("-")));
  }

  @VisibleForTesting
  static Optional<String> getDataprocRegion(ImmutableMap<String, String> conf) {
    return fetchGCPMetadata(DATAPROC_REGION_ENDPOINT, conf);
  }

  @VisibleForTesting
  static Optional<String> getDataprocJobID(ImmutableMap<String, String> conf) {
    return getPropertyFromYarnTag(conf, JOB_ID_PREFIX);
  }

  @VisibleForTesting
  static Optional<String> getDataprocJobUUID(ImmutableMap<String, String> conf) {
    return getPropertyFromYarnTag(conf, JOB_UUID_PREFIX);
  }

  @VisibleForTesting
  static Optional<String> getDataprocBatchID(ImmutableMap<String, String> conf) {
    return fetchGCPMetadata(BATCH_ID_ENDPOINT, conf);
  }

  @VisibleForTesting
  static Optional<String> getDataprocBatchUUID(ImmutableMap<String, String> conf) {
    return fetchGCPMetadata(BATCH_UUID_ENDPOINT, conf);
  }

  @VisibleForTesting
  static Optional<String> getDataprocSessionID(ImmutableMap<String, String> conf) {
    return fetchGCPMetadata(SESSION_ID_ENDPOINT, conf);
  }

  @VisibleForTesting
  private static Optional<String> getDataprocSessionUUID(ImmutableMap<String, String> conf) {
    return fetchGCPMetadata(SESSION_UUID_ENDPOINT, conf);
  }

  @VisibleForTesting
  static Optional<String> getGCPProjectId(ImmutableMap<String, String> conf) {
    return fetchGCPMetadata(PROJECT_ID_ENDPOINT, conf)
        .map(b -> b.substring(b.lastIndexOf('/') + 1));
  }

  @VisibleForTesting
  static Optional<String> getSparkAppId(ImmutableMap<String, String> conf) {
    return Optional.ofNullable(conf.get(SPARK_APP_ID));
  }

  @VisibleForTesting
  static Optional<String> getSparkAppName(ImmutableMap<String, String> conf) {
    return Optional.ofNullable(conf.get(SPARK_APP_NAME));
  }

  @VisibleForTesting
  static Optional<String> getClusterUUID(ImmutableMap<String, String> conf) {
    return fetchGCPMetadata(CLUSTER_UUID_ENDPOINT, conf);
  }

  @VisibleForTesting
  static Optional<String> getPropertyFromYarnTag(
      ImmutableMap<String, String> conf, String tagPrefix) {
    String yarnTag = conf.get(SPARK_YARN_TAGS);
    if (yarnTag == null) {
      return Optional.empty();
    }
    return Arrays.stream(yarnTag.split(","))
        .filter(tag -> tag.contains(tagPrefix))
        .findFirst()
        .map(tag -> tag.substring(tagPrefix.length()));
  }

  @VisibleForTesting
  static Optional<String> fetchGCPMetadata(String httpEndpoint, ImmutableMap<String, String> conf) {
    String baseUri = conf.getOrDefault(GOOGLE_METADATA_API, BASE_URI);
    String httpURI = baseUri + httpEndpoint;
    HttpGet httpGet = new HttpGet(httpURI);
    httpGet.addHeader(METADATA_FLAVOUR, GOOGLE);
    try {
      return HTTP_CLIENT.execute(
          httpGet,
          response -> {
            handleError(response);
            return Optional.of(EntityUtils.toString(response.getEntity()));
          });
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  private static void handleError(ClassicHttpResponse response) throws IOException, ParseException {
    final int statusCode = response.getCode();
    if (statusCode < 400 || statusCode >= 600) return;
    String message =
        String.format(
            "code: %d, response: %s",
            statusCode, EntityUtils.toString(response.getEntity(), UTF_8));
    throw new IOException(message);
  }
}
