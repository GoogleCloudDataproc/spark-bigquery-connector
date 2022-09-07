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
package com.google.cloud.bigquery.connector.common.integration;

import static com.google.common.truth.Truth.assertThat;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.AccessTokenProviderCredentials;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import java.util.Optional;
import org.junit.Test;

public class CustomCredentialsIntegrationTest {

  public static final TableId TABLE_ID =
      TableId.of("bigquery-public-data", "samples", "shakespeare");

  @Test
  public void testCredentialsProvider() throws Exception {
    BigQueryCredentialsSupplier credentialsSupplier =
        new BigQueryCredentialsSupplier(
            Optional.of(DefaultCredentialsDelegateCredentialsProvider.class.getCanonicalName()),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    Credentials credentials = credentialsSupplier.getCredentials();
    assertThat(credentials).isInstanceOf(DefaultCredentialsDelegateCredentials.class);
    DefaultCredentialsDelegateCredentials defaultCredentialsDelegateCredentials =
        (DefaultCredentialsDelegateCredentials) credentials;
    assertThat(defaultCredentialsDelegateCredentials.getCallCount()).isEqualTo(0);
    BigQueryOptions options = BigQueryOptions.newBuilder().setCredentials(credentials).build();
    BigQuery bigQuery = options.getService();
    // first call
    Table table = bigQuery.getTable(TABLE_ID);
    assertThat(table).isNotNull();
    assertThat(defaultCredentialsDelegateCredentials.getCallCount()).isEqualTo(1);
    // second call
    table = bigQuery.getTable(TABLE_ID);
    assertThat(table).isNotNull();
    assertThat(defaultCredentialsDelegateCredentials.getCallCount()).isEqualTo(2);
  }

  @Test
  public void testAccessTokenProvider() throws Exception {
    BigQueryCredentialsSupplier credentialsSupplier =
        new BigQueryCredentialsSupplier(
            Optional.empty(),
            Optional.of(DefaultCredentialsDelegateAccessTokenProvider.class.getCanonicalName()),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    Credentials credentials = credentialsSupplier.getCredentials();
    assertThat(credentials).isInstanceOf(AccessTokenProviderCredentials.class);
    DefaultCredentialsDelegateAccessTokenProvider accessTokenProvider =
        (DefaultCredentialsDelegateAccessTokenProvider)
            ((AccessTokenProviderCredentials) credentials).getAccessTokenProvider();
    assertThat(accessTokenProvider.getCallCount()).isEqualTo(0);
    BigQueryOptions options = BigQueryOptions.newBuilder().setCredentials(credentials).build();
    BigQuery bigQuery = options.getService();
    // first call
    Table table = bigQuery.getTable(TABLE_ID);
    assertThat(table).isNotNull();
    assertThat(accessTokenProvider.getCallCount()).isEqualTo(1);
    // second call
    table = bigQuery.getTable(TABLE_ID);
    assertThat(table).isNotNull();
    assertThat(accessTokenProvider.getCallCount()).isEqualTo(2);
  }
}
