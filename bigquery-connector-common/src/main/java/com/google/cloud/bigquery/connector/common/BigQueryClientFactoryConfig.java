/*
 * Copyright 2022 Google LLC
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

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class BigQueryClientFactoryConfig implements BigQueryConfig {

  private final Optional<String> accessTokenProviderFQCN;
  private final Optional<String> accessTokenProviderConfig;
  private final Optional<String> credentialsKey;
  private final Optional<String> credentialsFile;
  private final Optional<String> accessToken;
  private final String loggedInUserName;
  private final Set<String> loggedInUserGroups;
  private final Optional<String> impersonationServiceAccount;
  private final Optional<Map<String, String>> impersonationServiceAccountsForUsers;
  private final Optional<Map<String, String>> impersonationServiceAccountsForGroups;
  private final Optional<ImmutableList<String>> credentialsScopes;
  private final String parentProjectId;
  private final boolean useParentProjectForMetadataOperations;
  private final boolean viewsEnabled;
  private final int bigQueryClientConnectTimeout;
  private final int bigQueryClientReadTimeout;
  private final RetrySettings bigQueryClientRetrySettings;
  private final BigQueryProxyConfig bigQueryProxyConfig;
  private final Optional<String> bigQueryStorageGrpcEndpoint;
  private final Optional<String> bigQueryHttpEndpoint;
  private final int cacheExpirationTimeInMinutes;
  private final ImmutableMap<String, String> bigQueryJobLabels;
  private final Optional<Long> createReadSessionTimeoutInSeconds;
  private final int channelPoolSize;
  private final Optional<Integer> flowControlWindowBytes;
  private final QueryJobConfiguration.Priority queryJobPriority;
  private final long bigQueryJobTimeoutInMinutes;

  BigQueryClientFactoryConfig(BigQueryConfig bigQueryConfig, long bigQueryJobTimeoutInMinutes) {
    this.accessTokenProviderFQCN = bigQueryConfig.getAccessTokenProviderFQCN();
    this.accessTokenProviderConfig = bigQueryConfig.getAccessTokenProviderConfig();
    this.credentialsKey = bigQueryConfig.getCredentialsKey();
    this.credentialsFile = bigQueryConfig.getCredentialsFile();
    this.accessToken = bigQueryConfig.getAccessToken();
    this.loggedInUserName = bigQueryConfig.getLoggedInUserName();
    this.loggedInUserGroups = bigQueryConfig.getLoggedInUserGroups();
    this.impersonationServiceAccountsForUsers =
        bigQueryConfig.getImpersonationServiceAccountsForUsers();
    this.impersonationServiceAccountsForGroups =
        bigQueryConfig.getImpersonationServiceAccountsForGroups();
    this.impersonationServiceAccount = bigQueryConfig.getImpersonationServiceAccount();
    this.credentialsScopes = bigQueryConfig.getCredentialsScopes();
    this.parentProjectId = bigQueryConfig.getParentProjectId();
    this.useParentProjectForMetadataOperations =
        bigQueryConfig.useParentProjectForMetadataOperations();
    this.viewsEnabled = bigQueryConfig.isViewsEnabled();
    this.bigQueryClientConnectTimeout = bigQueryConfig.getBigQueryClientConnectTimeout();
    this.bigQueryClientReadTimeout = bigQueryConfig.getBigQueryClientReadTimeout();
    this.bigQueryClientRetrySettings = bigQueryConfig.getBigQueryClientRetrySettings();
    this.bigQueryProxyConfig = bigQueryConfig.getBigQueryProxyConfig();
    this.bigQueryStorageGrpcEndpoint = bigQueryConfig.getBigQueryStorageGrpcEndpoint();
    this.bigQueryHttpEndpoint = bigQueryConfig.getBigQueryHttpEndpoint();
    this.cacheExpirationTimeInMinutes = bigQueryConfig.getCacheExpirationTimeInMinutes();
    this.bigQueryJobLabels = bigQueryConfig.getBigQueryJobLabels();
    this.createReadSessionTimeoutInSeconds = bigQueryConfig.getCreateReadSessionTimeoutInSeconds();
    this.channelPoolSize = bigQueryConfig.getChannelPoolSize();
    this.flowControlWindowBytes = bigQueryConfig.getFlowControlWindowBytes();
    this.queryJobPriority = bigQueryConfig.getQueryJobPriority();
    this.bigQueryJobTimeoutInMinutes = bigQueryJobTimeoutInMinutes;
  }

  @Override
  public Optional<String> getAccessTokenProviderFQCN() {
    return accessTokenProviderFQCN;
  }

  @Override
  public Optional<String> getAccessTokenProviderConfig() {
    return accessTokenProviderConfig;
  }

  @Override
  public Optional<String> getCredentialsKey() {
    return credentialsKey;
  }

  @Override
  public Optional<String> getCredentialsFile() {
    return credentialsFile;
  }

  @Override
  public Optional<String> getAccessToken() {
    return accessToken;
  }

  @Override
  public String getLoggedInUserName() {
    return loggedInUserName;
  }

  @Override
  public Set<String> getLoggedInUserGroups() {
    return loggedInUserGroups;
  }

  @Override
  public Optional<Map<String, String>> getImpersonationServiceAccountsForUsers() {
    return impersonationServiceAccountsForUsers;
  }

  @Override
  public Optional<Map<String, String>> getImpersonationServiceAccountsForGroups() {
    return impersonationServiceAccountsForGroups;
  }

  @Override
  public Optional<String> getImpersonationServiceAccount() {
    return impersonationServiceAccount;
  }

  @Override
  public Optional<ImmutableList<String>> getCredentialsScopes() {
    return credentialsScopes;
  }

  @Override
  public String getParentProjectId() {
    return parentProjectId;
  }

  @Override
  public boolean useParentProjectForMetadataOperations() {
    return useParentProjectForMetadataOperations;
  }

  @Override
  public boolean isViewsEnabled() {
    return viewsEnabled;
  }

  @Override
  public int getBigQueryClientConnectTimeout() {
    return bigQueryClientConnectTimeout;
  }

  @Override
  public int getBigQueryClientReadTimeout() {
    return bigQueryClientReadTimeout;
  }

  @Override
  public RetrySettings getBigQueryClientRetrySettings() {
    return bigQueryClientRetrySettings;
  }

  @Override
  public BigQueryProxyConfig getBigQueryProxyConfig() {
    return bigQueryProxyConfig;
  }

  @Override
  public Optional<String> getBigQueryStorageGrpcEndpoint() {
    return bigQueryStorageGrpcEndpoint;
  }

  @Override
  public Optional<String> getBigQueryHttpEndpoint() {
    return bigQueryHttpEndpoint;
  }

  @Override
  public int getCacheExpirationTimeInMinutes() {
    return cacheExpirationTimeInMinutes;
  }

  @Override
  public ImmutableMap<String, String> getBigQueryJobLabels() {
    return bigQueryJobLabels;
  }

  @Override
  public Optional<Long> getCreateReadSessionTimeoutInSeconds() {
    return createReadSessionTimeoutInSeconds;
  }

  @Override
  public int getChannelPoolSize() {
    return channelPoolSize;
  }

  @Override
  public Optional<Integer> getFlowControlWindowBytes() {
    return flowControlWindowBytes;
  }

  @Override
  public Priority getQueryJobPriority() {
    return queryJobPriority;
  }

  public long getBigQueryJobTimeoutInMinutes() {
    return bigQueryJobTimeoutInMinutes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BigQueryClientFactoryConfig)) {
      return false;
    }
    BigQueryClientFactoryConfig that = (BigQueryClientFactoryConfig) o;
    return viewsEnabled == that.viewsEnabled
        && bigQueryClientConnectTimeout == that.bigQueryClientConnectTimeout
        && bigQueryClientReadTimeout == that.bigQueryClientReadTimeout
        && Objects.equal(credentialsKey, that.credentialsKey)
        && Objects.equal(credentialsFile, that.credentialsFile)
        && Objects.equal(accessToken, that.accessToken)
        && Objects.equal(parentProjectId, that.parentProjectId)
        && Objects.equal(
            useParentProjectForMetadataOperations, that.useParentProjectForMetadataOperations)
        && Objects.equal(bigQueryClientRetrySettings, that.bigQueryClientRetrySettings)
        && Objects.equal(bigQueryProxyConfig, that.bigQueryProxyConfig)
        && Objects.equal(bigQueryStorageGrpcEndpoint, that.bigQueryStorageGrpcEndpoint)
        && Objects.equal(bigQueryHttpEndpoint, that.bigQueryHttpEndpoint)
        && Objects.equal(cacheExpirationTimeInMinutes, that.cacheExpirationTimeInMinutes)
        && Objects.equal(createReadSessionTimeoutInSeconds, that.createReadSessionTimeoutInSeconds)
        && Objects.equal(channelPoolSize, that.channelPoolSize)
        && Objects.equal(flowControlWindowBytes, that.flowControlWindowBytes);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        credentialsKey,
        credentialsFile,
        accessToken,
        parentProjectId,
        useParentProjectForMetadataOperations,
        viewsEnabled,
        bigQueryClientConnectTimeout,
        bigQueryClientReadTimeout,
        bigQueryClientRetrySettings,
        bigQueryProxyConfig,
        bigQueryStorageGrpcEndpoint,
        bigQueryHttpEndpoint,
        cacheExpirationTimeInMinutes,
        channelPoolSize,
        flowControlWindowBytes);
  }
}
