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
package com.google.cloud.bigquery.connector.common;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface BigQueryConfig {

  Optional<String> getAccessTokenProviderFQCN();

  Optional<String> getAccessTokenProviderConfig();

  Optional<String> getCredentialsKey();

  Optional<String> getCredentialsFile();

  Optional<String> getAccessToken();

  String getLoggedInUserName();

  Set<String> getLoggedInUserGroups();

  Optional<Map<String, String>> getImpersonationServiceAccountsForUsers();

  Optional<Map<String, String>> getImpersonationServiceAccountsForGroups();

  Optional<String> getImpersonationServiceAccount();

  String getParentProjectId();

  Optional<String> getCatalogProjectId();

  Optional<String> getCatalogLocation();

  boolean useParentProjectForMetadataOperations();

  boolean isViewsEnabled();

  Optional<String> getMaterializationProject();

  Optional<String> getMaterializationDataset();

  int getBigQueryClientConnectTimeout();

  int getBigQueryClientReadTimeout();

  RetrySettings getBigQueryClientRetrySettings();

  BigQueryProxyConfig getBigQueryProxyConfig();

  Optional<String> getBigQueryStorageGrpcEndpoint();

  Optional<String> getBigQueryHttpEndpoint();

  int getCacheExpirationTimeInMinutes();

  ImmutableMap<String, String> getBigQueryJobLabels();

  Optional<Long> getCreateReadSessionTimeoutInSeconds();

  int getChannelPoolSize();

  // Get a static flow control window per RPC. When not set
  // auto flow control is determined by Bandwidth Delay Product.
  Optional<Integer> getFlowControlWindowBytes();

  Priority getQueryJobPriority();

  long getBigQueryJobTimeoutInMinutes();

  Optional<ImmutableList<String>> getCredentialsScopes();

  default int getClientCreationHashCode() {
    return Objects.hashCode(
        getAccessTokenProviderFQCN(),
        getAccessTokenProviderConfig(),
        getCredentialsKey(),
        getAccessToken(),
        getCredentialsFile(),
        getBigQueryHttpEndpoint(),
        getFlowControlWindowBytes(),
        getBigQueryStorageGrpcEndpoint(),
        getCreateReadSessionTimeoutInSeconds(),
        getBigQueryProxyConfig(),
        getParentProjectId(),
        useParentProjectForMetadataOperations());
  }

  default boolean areClientCreationConfigsEqual(BigQueryConfig b) {
    if (this == b) {
      return true;
    }
    return Objects.equal(getAccessTokenProviderFQCN(), b.getAccessTokenProviderFQCN())
        && Objects.equal(getAccessTokenProviderConfig(), b.getAccessTokenProviderConfig())
        && Objects.equal(getCredentialsKey(), b.getCredentialsKey())
        && Objects.equal(getAccessToken(), b.getAccessToken())
        && Objects.equal(getCredentialsFile(), b.getCredentialsFile())
        && Objects.equal(getBigQueryHttpEndpoint(), b.getBigQueryHttpEndpoint())
        && Objects.equal(getFlowControlWindowBytes(), b.getFlowControlWindowBytes())
        && Objects.equal(getBigQueryStorageGrpcEndpoint(), b.getBigQueryStorageGrpcEndpoint())
        && Objects.equal(
            getCreateReadSessionTimeoutInSeconds(), b.getCreateReadSessionTimeoutInSeconds())
        && Objects.equal(getBigQueryProxyConfig(), b.getBigQueryProxyConfig())
        && Objects.equal(getParentProjectId(), b.getParentProjectId())
        && Objects.equal(
            useParentProjectForMetadataOperations(), b.useParentProjectForMetadataOperations());
  }
}
