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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterConfig;
import com.google.cloud.dataproc.v1.Component;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobStatus;
import com.google.cloud.dataproc.v1.NodeInitializationAction;
import com.google.cloud.spark.bigquery.integration.TestConstants;
import com.google.cloud.spark.bigquery.util.TestUtils;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class BigQueryFederationAcceptanceTestBase extends AcceptanceTestBase {

  public static final String METASTORE_WAREHOUSE_DIR =
      TestUtils.getEnvironmentVariableOrFail("METASTORE_WAREHOUSE_DIR");
  public static final String METASTORE_PROXY_URI =
      TestUtils.getEnvironmentVariableOrFail("METASTORE_PROXY_URI");

  protected BigQueryFederationAcceptanceTestBase(AcceptanceTestContext context) {
    super(context);
  }

  protected BigQueryFederationAcceptanceTestBase(
      AcceptanceTestContext context, boolean sparkStreamingSupported) {
    super(context, sparkStreamingSupported);
  }

  protected static AcceptanceTestContext setup(
      String dataprocImageVersion,
      String connectorJarPrefix,
      List<ClusterProperty> clusterProperties)
      throws Exception {
    return setup(
        dataprocImageVersion,
        connectorJarPrefix,
        clusterProperties,
        BigQueryFederationAcceptanceTestBase::customizeBigQueryFederation);
  }

  public static Cluster customizeBigQueryFederation(Cluster cluster) {
    ClusterConfig.Builder config = cluster.getConfig().toBuilder();
    config
        .addInitializationActions(
            NodeInitializationAction.newBuilder()
                .setExecutableFile(
                    "gs://metastore-init-actions/metastore-grpc-proxy/metastore-grpc-proxy.sh"))
        .setGceClusterConfig(
            config
                .getGceClusterConfig()
                .toBuilder()
                .putMetadata("proxy-uri", METASTORE_PROXY_URI)
                .putMetadata("hive-version", "3.1.2")
                .addServiceAccountScopes("https://www.googleapis.com/auth/cloud-platform"))
        .setSoftwareConfig(
            config
                .getSoftwareConfig()
                .toBuilder()
                .putProperties("hive:hive.metastore.uris", "thrift://localhost:9083")
                .putProperties("hive:hive.metastore.warehouse.dir", METASTORE_WAREHOUSE_DIR)
                .addOptionalComponents(Component.DOCKER));

    return cluster
        .toBuilder()
        .setClusterName(cluster.getClusterName() + "-bq-fed")
        .setConfig(config)
        .build();
  }

  @Test
  public void testBigQueryFederation() throws Exception {
    String testName = "bigquery-federation";
    Job result =
        createAndRunPythonJob(
            testName,
            "bigquery_federation.py",
            null,
            Arrays.asList(TestConstants.TEMPORARY_GCS_BUCKET, context.getResultsDirUri(testName)),
            120);
    assertThat(result.getStatus().getState()).isEqualTo(JobStatus.State.DONE);
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo("spark,10");
  }
}
