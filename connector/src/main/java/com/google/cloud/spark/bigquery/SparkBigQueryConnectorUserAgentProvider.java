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
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.UserAgentProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import scala.util.Properties;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Provides the versions of the client environment in an anonymous way.
 */
public class SparkBigQueryConnectorUserAgentProvider implements UserAgentProvider {

    private static String CONNECTOR_VERSION = BuildInfo.version();
    private static String SPARK_VERSION = SparkSession.builder().getOrCreate().version();
    private static String JAVA_VERSION = System.getProperty("java.runtime.version");
    private static String SCALA_VERSION = Properties.versionNumberString();


    @VisibleForTesting
    static String GCP_REGION_PART = getGcpRegion().map(region -> " region/" + region).orElse("");

    @VisibleForTesting
    static String DATAPROC_IMAGE_PART = Optional.ofNullable(System.getenv("DATAPROC_IMAGE_VERSION"))
            .map(image -> " dataproc-image/" + image)
            .orElse("");

    static final String USER_AGENT = format("spark-bigquery-connector/%s spark/%s java/%s scala/%s%s%s",
            CONNECTOR_VERSION,
            SPARK_VERSION,
            JAVA_VERSION,
            SCALA_VERSION,
            GCP_REGION_PART,
            DATAPROC_IMAGE_PART
    );

    private SparkContext sparkContext;

    public SparkBigQueryConnectorUserAgentProvider() {}

    public SparkBigQueryConnectorUserAgentProvider(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    @Override
    public String getUserAgent() {
        return USER_AGENT;
    }

    // Queries the GCE metadata server
    @VisibleForTesting
    static Optional<String> getGcpRegion() {
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(100)
                .setConnectionRequestTimeout(100)
                .setSocketTimeout(100).build();
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();
        HttpGet httpGet = new HttpGet("http://metadata.google.internal/computeMetadata/v1/instance/zone");
        httpGet.addHeader("Metadata-Flavor", "Google");
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            if (response.getStatusLine().getStatusCode() == 200) {
                String body = CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), UTF_8));
                return Optional.of(body.substring(body.lastIndexOf('/') + 1));
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            return Optional.empty();
        } finally {
            try {
                Closeables.close(httpClient, true);
            } catch (IOException e) {
                // nothing to do
            }
        }
    }
}
