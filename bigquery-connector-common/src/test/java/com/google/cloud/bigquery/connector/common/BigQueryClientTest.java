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
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.http.HttpTransportOptions;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import java.util.OptionalLong;
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

  private static final TableInfo TABLE =
      TableInfo.of(TableId.of("p", "d", "t"), StandardTableDefinition.of(Schema.of()));

  @Test
  public void testTableSizeSqlWithoutSnapshotTime() {
    assertThat(BigQueryClient.createTableSizeSql(TABLE, Optional.of("x > 1"), OptionalLong.empty()))
        .doesNotContain("FOR SYSTEM_TIME");
  }

  @Test
  public void testTableSizeSqlWithSnapshotTime() {
    assertThat(BigQueryClient.createTableSizeSql(TABLE, Optional.empty(), OptionalLong.of(1234L)))
        .contains("`p.d.t` FOR SYSTEM_TIME AS OF TIMESTAMP_MILLIS(1234)");
    // The clause belongs to the table reference, before the WHERE clause.
    assertThat(
            BigQueryClient.createTableSizeSql(TABLE, Optional.of("x > 1"), OptionalLong.of(1234L)))
        .contains("`p.d.t` FOR SYSTEM_TIME AS OF TIMESTAMP_MILLIS(1234) WHERE x > 1");
  }

  private static BigQueryClient newClient() {
    BigQueryOptions options =
        BigQueryOptions.newBuilder()
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .build();
    return new BigQueryClient(
        options.getService(),
        Optional.empty(),
        Optional.empty(),
        CacheBuilder.newBuilder().build(),
        ImmutableMap.of(),
        Priority.INTERACTIVE,
        Optional.empty(),
        60);
  }

  @Test
  public void testCreateSqlWithoutSnapshotTime() {
    assertThat(
            newClient()
                .createSql(
                    TableId.of("p", "d", "v"),
                    ImmutableList.of("x"),
                    new String[] {"x > 1"},
                    OptionalLong.empty()))
        .isEqualTo("SELECT `x` FROM `p.d.v` WHERE (x > 1)");
  }

  @Test
  public void testCreateSqlWithSnapshotTime() {
    assertThat(
            newClient()
                .createSql(
                    TableId.of("p", "d", "v"),
                    ImmutableList.of(),
                    new String[0],
                    OptionalLong.of(1234L)))
        .isEqualTo("SELECT * FROM `p.d.v` FOR SYSTEM_TIME AS OF TIMESTAMP_MILLIS(1234)");
  }

  @Test
  public void testCreateSqlWithSnapshotTimeAndFilter() {
    // The clause belongs to the table reference, before the WHERE clause.
    assertThat(
            newClient()
                .createSql(
                    TableId.of("p", "d", "v"),
                    ImmutableList.of("x"),
                    new String[] {"x > 1"},
                    OptionalLong.of(1234L)))
        .isEqualTo(
            "SELECT `x` FROM `p.d.v` FOR SYSTEM_TIME AS OF TIMESTAMP_MILLIS(1234) WHERE (x > 1)");
  }

  @Test
  public void testTableSizeSqlWithSnapshotTimeIgnoresViews() {
    TableInfo view = TableInfo.of(TableId.of("p", "d", "v"), ViewDefinition.of("SELECT 1"));
    assertThat(
            BigQueryClient.createTableSizeSql(view, Optional.of("x > 1"), OptionalLong.of(1234L)))
        .doesNotContain("FOR SYSTEM_TIME");
  }
}
