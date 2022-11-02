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

import com.google.api.gax.rpc.HeaderProvider;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.http.HttpTransportOptions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import java.util.concurrent.TimeUnit;

public class BigQueryClientModule implements com.google.inject.Module {

  private static final int DESTINATION_TABLE_CACHE_MAX_SIZE = 1000;

  /*
   * In order to parameterize the cache expiration time, the instance needs to be loaded lazily.
   * The instance is marked as static so that the instance is a singleton.
   */
  private static Cache<String, TableInfo> cacheInstance;

  @Provides
  @Singleton
  public static HeaderProvider createHeaderProvider(
      BigQueryConfig config, UserAgentProvider userAgentProvider) {
    return HttpUtil.createHeaderProvider(config, userAgentProvider.getUserAgent());
  }

  @Override
  public void configure(Binder binder) {
    // BigQuery related
    binder.bind(BigQueryClientFactory.class).in(Scopes.SINGLETON);
    binder
        .bind(BigQueryTracerFactory.class)
        .toInstance(new LoggingBigQueryTracerFactory(/*Log every 2^14 batches*/ 14));
  }

  @Provides
  @Singleton
  public BigQueryCredentialsSupplier provideBigQueryCredentialsSupplier(BigQueryConfig config) {
    BigQueryProxyConfig proxyConfig = config.getBigQueryProxyConfig();
    return new BigQueryCredentialsSupplier(
        config.getAccessTokenProviderFQCN(),
        config.getAccessTokenProviderConfig(),
        config.getAccessToken(),
        config.getCredentialsKey(),
        config.getCredentialsFile(),
        proxyConfig.getProxyUri(),
        proxyConfig.getProxyUsername(),
        proxyConfig.getProxyPassword());
  }

  @Provides
  @Singleton
  public Cache<String, TableInfo> provideDestinationTableCache(BigQueryConfig config) {
    if (cacheInstance == null) {
      synchronized (BigQueryClientModule.class) {
        if (cacheInstance == null) {
          cacheInstance =
              CacheBuilder.newBuilder()
                  .expireAfterWrite(config.getCacheExpirationTimeInMinutes(), TimeUnit.MINUTES)
                  .maximumSize(DESTINATION_TABLE_CACHE_MAX_SIZE)
                  .build();
        }
      }
    }

    return cacheInstance;
  }

  @Provides
  @Singleton
  public BigQueryClient provideBigQueryClient(
      BigQueryConfig config,
      HeaderProvider headerProvider,
      BigQueryCredentialsSupplier bigQueryCredentialsSupplier,
      Cache<String, TableInfo> destinationTableCache) {
    BigQueryOptions.Builder options =
        BigQueryOptions.newBuilder()
            .setHeaderProvider(headerProvider)
            .setProjectId(config.getParentProjectId())
            .setCredentials(bigQueryCredentialsSupplier.getCredentials())
            .setRetrySettings(config.getBigQueryClientRetrySettings());

    HttpTransportOptions.Builder httpTransportOptionsBuilder =
        HttpTransportOptions.newBuilder()
            .setConnectTimeout(config.getBigQueryClientConnectTimeout())
            .setReadTimeout(config.getBigQueryClientReadTimeout());
    BigQueryProxyConfig proxyConfig = config.getBigQueryProxyConfig();
    if (proxyConfig.getProxyUri().isPresent()) {
      httpTransportOptionsBuilder.setHttpTransportFactory(
          BigQueryProxyTransporterBuilder.createHttpTransportFactory(
              proxyConfig.getProxyUri(),
              proxyConfig.getProxyUsername(),
              proxyConfig.getProxyPassword()));
    }

    config.getBigQueryHttpEndpoint().ifPresent(options::setHost);

    options.setTransportOptions(httpTransportOptionsBuilder.build());
    return new BigQueryClient(
        options.build().getService(),
        config.getMaterializationProject(),
        config.getMaterializationDataset(),
        destinationTableCache,
        config.getBigQueryJobLabels());
  }
}
