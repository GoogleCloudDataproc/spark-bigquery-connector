package com.google.cloud.bigquery.connector.common;

import com.google.api.gax.retrying.RetrySettings;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;

public class BigQueryClientFactoryConfig implements BigQueryConfig {

  private final Optional<String> accessTokenProviderFQCN;
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
  private final Optional<String> endpoint;
  private final int cacheExpirationTimeInMinutes;
  private final ImmutableMap<String, String> bigQueryJobLabels;

  BigQueryClientFactoryConfig(BigQueryConfig bigQueryConfig) {
    this.accessTokenProviderFQCN = bigQueryConfig.getAccessTokenProviderFQCN();
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
    this.endpoint = bigQueryConfig.getEndpoint();
    this.cacheExpirationTimeInMinutes = bigQueryConfig.getCacheExpirationTimeInMinutes();
    this.bigQueryJobLabels = bigQueryConfig.getBigQueryJobLabels();
  }

  @Override
  public Optional<String> getAccessTokenProviderFQCN() {
    return accessTokenProviderFQCN;
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
  public Optional<String> getEndpoint() {
    return endpoint;
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
        && Objects.equal(endpoint, that.endpoint)
        && Objects.equal(cacheExpirationTimeInMinutes, that.cacheExpirationTimeInMinutes);
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
        endpoint,
        cacheExpirationTimeInMinutes);
  }
}
