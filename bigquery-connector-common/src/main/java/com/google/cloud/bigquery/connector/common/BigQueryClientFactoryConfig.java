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
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;

public class BigQueryClientFactoryConfig implements BigQueryConfig {

  private final Optional<String> accessTokenProviderFQCN;
  private final Optional<String> accessTokenProviderConfig;
  private final Optional<String> credentialsKey;
  private final Optional<String> credentialsFile;
  private final Optional<String> accessToken;
  private final String parentProjectId;
  private final boolean useParentProjectForMetadataOperations;
  private final boolean viewsEnabled;
  private final Optional<String> materializationProject;
  private final Optional<String> materializationDataset;
  private final int bigQueryClientConnectTimeout;
  private final int bigQueryClientReadTimeout;
  private final RetrySettings bigQueryClientRetrySettings;
  private final BigQueryProxyConfig bigQueryProxyConfig;
  private final Optional<String> bigQueryStorageGrpcEndpoint;
  private final Optional<String> bigQueryHttpEndpoint;
  private final int cacheExpirationTimeInMinutes;
  private final ImmutableMap<String, String> bigQueryJobLabels;
  private final Optional<Long> createReadSessionTimeoutInSeconds;

  BigQueryClientFactoryConfig(BigQueryConfig bigQueryConfig) {
    this.accessTokenProviderFQCN = bigQueryConfig.getAccessTokenProviderFQCN();
    this.accessTokenProviderConfig = bigQueryConfig.getAccessTokenProviderConfig();
    this.credentialsKey = bigQueryConfig.getCredentialsKey();
    this.credentialsFile = bigQueryConfig.getCredentialsFile();
    this.accessToken = bigQueryConfig.getAccessToken();
    this.parentProjectId = bigQueryConfig.getParentProjectId();
    this.useParentProjectForMetadataOperations =
        bigQueryConfig.useParentProjectForMetadataOperations();
    this.viewsEnabled = bigQueryConfig.isViewsEnabled();
    this.materializationProject = bigQueryConfig.getMaterializationProject();
    this.materializationDataset = bigQueryConfig.getMaterializationDataset();
    this.bigQueryClientConnectTimeout = bigQueryConfig.getBigQueryClientConnectTimeout();
    this.bigQueryClientReadTimeout = bigQueryConfig.getBigQueryClientReadTimeout();
    this.bigQueryClientRetrySettings = bigQueryConfig.getBigQueryClientRetrySettings();
    this.bigQueryProxyConfig = bigQueryConfig.getBigQueryProxyConfig();
    this.bigQueryStorageGrpcEndpoint = bigQueryConfig.getBigQueryStorageGrpcEndpoint();
    this.bigQueryHttpEndpoint = bigQueryConfig.getBigQueryHttpEndpoint();
    this.cacheExpirationTimeInMinutes = bigQueryConfig.getCacheExpirationTimeInMinutes();
    this.bigQueryJobLabels = bigQueryConfig.getBigQueryJobLabels();
    this.createReadSessionTimeoutInSeconds = bigQueryConfig.getCreateReadSessionTimeoutInSeconds();
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
  public Optional<String> getMaterializationProject() {
    return materializationProject;
  }

  @Override
  public Optional<String> getMaterializationDataset() {
    return materializationDataset;
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
        && Objects.equal(materializationProject, that.materializationProject)
        && Objects.equal(materializationDataset, that.materializationDataset)
        && Objects.equal(bigQueryClientRetrySettings, that.bigQueryClientRetrySettings)
        && Objects.equal(bigQueryProxyConfig, that.bigQueryProxyConfig)
        && Objects.equal(bigQueryStorageGrpcEndpoint, that.bigQueryStorageGrpcEndpoint)
        && Objects.equal(bigQueryHttpEndpoint, that.bigQueryHttpEndpoint)
        && Objects.equal(cacheExpirationTimeInMinutes, that.cacheExpirationTimeInMinutes)
        && Objects.equal(createReadSessionTimeoutInSeconds, that.createReadSessionTimeoutInSeconds);
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
        materializationProject,
        materializationDataset,
        bigQueryClientConnectTimeout,
        bigQueryClientReadTimeout,
        bigQueryClientRetrySettings,
        bigQueryProxyConfig,
        bigQueryStorageGrpcEndpoint,
        bigQueryHttpEndpoint,
        cacheExpirationTimeInMinutes);
  }
}
