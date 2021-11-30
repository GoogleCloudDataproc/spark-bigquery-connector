package com.google.cloud.bigquery.connector.common;

import com.google.api.gax.retrying.RetrySettings;
import com.google.common.base.Objects;
import java.util.Optional;

public class BigQueryClientFactoryConfig implements BigQueryConfig {

  private Optional<String> credentialsKey;
  private Optional<String> credentialsFile;
  private Optional<String> accessToken;
  private String parentProjectId;
  private boolean viewsEnabled;
  private Optional<String> materializationProject;
  private Optional<String> materializationDataset;
  private int bigQueryClientConnectTimeout;
  private int bigQueryClientReadTimeout;
  private RetrySettings bigQueryClientRetrySettings;
  private BigQueryProxyConfig bigQueryProxyConfig;
  private Optional<String> endpoint;

  BigQueryClientFactoryConfig() {
    // empty
  }

  public static BigQueryClientFactoryConfig from(BigQueryConfig bigQueryConfig) {
    BigQueryClientFactoryConfig config = new BigQueryClientFactoryConfig();
    config.credentialsKey = bigQueryConfig.getCredentialsKey();
    config.credentialsFile = bigQueryConfig.getCredentialsFile();
    config.accessToken = bigQueryConfig.getAccessToken();
    config.parentProjectId = bigQueryConfig.getParentProjectId();
    config.viewsEnabled = bigQueryConfig.isViewsEnabled();
    config.materializationProject = bigQueryConfig.getMaterializationProject();
    config.materializationDataset = bigQueryConfig.getMaterializationDataset();
    config.bigQueryClientConnectTimeout = bigQueryConfig.getBigQueryClientConnectTimeout();
    config.bigQueryClientReadTimeout = bigQueryConfig.getBigQueryClientReadTimeout();
    config.bigQueryClientRetrySettings = bigQueryConfig.getBigQueryClientRetrySettings();
    config.bigQueryProxyConfig = bigQueryConfig.getBigQueryProxyConfig();
    config.endpoint = bigQueryConfig.getEndpoint();

    return config;
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
        && Objects.equal(materializationProject, that.materializationProject)
        && Objects.equal(materializationDataset, that.materializationDataset)
        && Objects.equal(bigQueryClientRetrySettings, that.bigQueryClientRetrySettings)
        && Objects.equal(bigQueryProxyConfig, that.bigQueryProxyConfig)
        && Objects.equal(endpoint, that.endpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        credentialsKey,
        credentialsFile,
        accessToken,
        parentProjectId,
        viewsEnabled,
        materializationProject,
        materializationDataset,
        bigQueryClientConnectTimeout,
        bigQueryClientReadTimeout,
        bigQueryClientRetrySettings,
        bigQueryProxyConfig,
        endpoint);
  }
}
