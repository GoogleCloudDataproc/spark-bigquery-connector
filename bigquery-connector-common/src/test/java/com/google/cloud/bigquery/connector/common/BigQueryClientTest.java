/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;
import org.junit.Test;

public class BigQueryClientTest {

  @Test
  public void testRestTransportUsesConfiguredHttpTransportFactory() {
    HttpTransport transport = new NetHttpTransport();
    BigQueryOptions options =
        BigQueryOptions.newBuilder()
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .setTransportOptions(
                HttpTransportOptions.newBuilder().setHttpTransportFactory(() -> transport).build())
            .build();

    assertThat(BigQueryClient.createRestTransport(options)).isSameInstanceAs(transport);
  }

  @Test
  public void testRestTransportDefaultsToNetHttpTransport() {
    BigQueryOptions options =
        BigQueryOptions.newBuilder()
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .build();

    assertThat(BigQueryClient.createRestTransport(options)).isInstanceOf(NetHttpTransport.class);
  }

  @Test
  public void testRestTransportWithHttpTransportOptionsWithoutFactory() {
    BigQueryOptions options =
        BigQueryOptions.newBuilder()
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .setTransportOptions(HttpTransportOptions.newBuilder().setConnectTimeout(1000).build())
            .build();

    assertThat(BigQueryClient.createRestTransport(options)).isInstanceOf(NetHttpTransport.class);
  }

  @Test
  public void testRestClientUsesBigQueryTransport() {
    HttpTransport transport = new NetHttpTransport();
    BigQueryOptions options =
        BigQueryOptions.newBuilder()
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .setTransportOptions(
                HttpTransportOptions.newBuilder().setHttpTransportFactory(() -> transport).build())
            .build();

    assertThat(
            BigQueryClient.createRestClient(options.getService())
                .getRequestFactory()
                .getTransport())
        .isSameInstanceAs(transport);
  }
}
