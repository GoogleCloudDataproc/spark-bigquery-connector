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

import com.google.api.core.ApiFunction;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.base.Objects;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * Since Guice recommends to avoid injecting closeable resources (see
 * https://github.com/google/guice/wiki/Avoid-Injecting-Closable-Resources), this factory creates
 * and caches clients and also closes them during JVM shutdown.
 */
public class BigQueryClientFactory implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(BigQueryClientFactory.class);
  private static final Map<BigQueryClientFactory, BigQueryReadClient> readClientMap =
      new HashMap<>();
  private static final Map<BigQueryClientFactory, BigQueryWriteClient> writeClientMap =
      new HashMap<>();

  // using the user agent as HeaderProvider is not serializable
  private final HeaderProvider headerProvider;
  private final BigQueryConfig bqConfig;

  // GoogleCredentials are not compatible with Kryo serialization, so we serialize and deserialize
  // when needed
  private final byte[] serializedCredentials;
  private transient volatile Credentials credentials;

  private int cachedHashCode = 0;
  private IdentityTokenSupplier identityTokenSupplier;

  @Inject
  public BigQueryClientFactory(
      BigQueryCredentialsSupplier bigQueryCredentialsSupplier,
      HeaderProvider headerProvider,
      BigQueryConfig bqConfig) {
    // using Guava's optional as it is serializable
    this.credentials = bigQueryCredentialsSupplier.getCredentials();
    this.serializedCredentials = BigQueryUtil.getCredentialsByteArray(credentials);
    this.headerProvider = headerProvider;
    this.bqConfig = bqConfig;
  }

  public BigQueryReadClient getBigQueryReadClient() {
    synchronized (readClientMap) {
      if (!readClientMap.containsKey(this)) {
        BigQueryReadClient bigQueryReadClient =
            createBigQueryReadClient(
                this.bqConfig.getBigQueryStorageGrpcEndpoint(),
                this.bqConfig.getChannelPoolSize(),
                this.bqConfig.getFlowControlWindowBytes());
        Runtime.getRuntime()
            .addShutdownHook(new Thread(() -> shutdownBigQueryReadClient(bigQueryReadClient)));
        readClientMap.put(this, bigQueryReadClient);
      }
    }

    return readClientMap.get(this);
  }

  public BigQueryWriteClient getBigQueryWriteClient() {
    synchronized (writeClientMap) {
      if (!writeClientMap.containsKey(this)) {
        BigQueryWriteClient bigQueryWriteClient =
            createBigQueryWriteClient(this.bqConfig.getBigQueryStorageGrpcEndpoint());
        Runtime.getRuntime()
            .addShutdownHook(new Thread(() -> shutdownBigQueryWriteClient(bigQueryWriteClient)));
        writeClientMap.put(this, bigQueryWriteClient);
      }
    }

    return writeClientMap.get(this);
  }

  @Override
  public int hashCode() {
    // caching the hash code, as we use it 3 times (potentially) get*Client() method
    if (cachedHashCode == 0) {
      // Here, credentials is an instance of GoogleCredentials which can be one out of
      // GoogleCredentials, UserCredentials, ServiceAccountCredentials, ExternalAccountCredentials
      // or ImpersonatedCredentials (See the class BigQueryCredentialsSupplier which supplies these
      // Credentials). Subclasses of the abstract class ExternalAccountCredentials do not have the
      // hashCode method defined on them and hence we get the byte array of the
      // ExternalAccountCredentials first and then compare their hashCodes.
      if (getCredentials() instanceof ExternalAccountCredentials) {
        cachedHashCode =
            Objects.hashCode(
                Arrays.hashCode(serializedCredentials),
                headerProvider,
                bqConfig.getClientCreationHashCode());
      } else {
        cachedHashCode =
            Objects.hashCode(
                getCredentials(),
                headerProvider,
                bqConfig.getClientCreationHashCode(),
                identityTokenSupplier);
      }
    }
    return cachedHashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BigQueryClientFactory)) {
      return false;
    }

    BigQueryClientFactory that = (BigQueryClientFactory) o;

    if (Objects.equal(headerProvider, that.headerProvider)
        && bqConfig.areClientCreationConfigsEqual(that.bqConfig)) {
      // Here, credentials and that.credentials are instances of GoogleCredentials which can be one
      // of GoogleCredentials, UserCredentials, ServiceAccountCredentials,
      // ExternalAccountCredentials or ImpersonatedCredentials (See the class
      // BigQueryCredentialsSupplier which supplies these Credentials). Subclasses of
      // ExternalAccountCredentials do not have an equals method defined on them and hence we
      // serialize and compare byte arrays if either of the credentials are instances of
      // ExternalAccountCredentials
      return BigQueryUtil.areCredentialsEqual(getCredentials(), that.getCredentials());
    }

    return false;
  }

  private Credentials getCredentials() {
    if (credentials == null) {
      synchronized (BigQueryClientFactory.class) {
        if (credentials == null) {
          credentials = BigQueryUtil.getCredentialsFromByteArray(serializedCredentials);
        }
      }
    }
    return credentials;
  }

  private BigQueryReadClient createBigQueryReadClient(
      Optional<String> endpoint, int channelPoolSize, Optional<Integer> flowControlWindow) {
    try {
      InstantiatingGrpcChannelProvider.Builder transportBuilder = createTransportBuilder(endpoint);
      log.info("Channel pool size set to {}", channelPoolSize);
      transportBuilder.setChannelPoolSettings(ChannelPoolSettings.staticallySized(channelPoolSize));
      if (flowControlWindow.isPresent()) {
        ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> channelConfigurator =
            (ManagedChannelBuilder channelBuilder) -> {
              if (channelBuilder instanceof NettyChannelBuilder) {
                log.info("Flow control window for netty set to {} bytes", flowControlWindow.get());
                return ((NettyChannelBuilder) channelBuilder)
                    .flowControlWindow(flowControlWindow.get());
              } else {
                log.info("Flow control window configured but underlying channel is not Netty");
              }
              return channelBuilder;
            };

        transportBuilder = transportBuilder.setChannelConfigurator(channelConfigurator);
      }
      BigQueryReadSettings.Builder clientSettings =
          BigQueryReadSettings.newBuilder()
              .setTransportChannelProvider(transportBuilder.build())
              .setCredentialsProvider(FixedCredentialsProvider.create(getCredentials()));

      bqConfig
          .getCreateReadSessionTimeoutInSeconds()
          .ifPresent(
              timeoutInSeconds -> {
                // Setting the read session timeout only if the user provided one using options or
                // using the default timeout
                UnaryCallSettings.Builder<CreateReadSessionRequest, ReadSession>
                    createReadSessionSettings =
                        clientSettings.getStubSettingsBuilder().createReadSessionSettings();
                Duration timeout = Duration.ofSeconds(timeoutInSeconds);
                createReadSessionSettings.setRetrySettings(
                    createReadSessionSettings
                        .getRetrySettings()
                        .toBuilder()
                        .setInitialRpcTimeout(timeout)
                        .setMaxRpcTimeout(timeout)
                        .setTotalTimeout(timeout)
                        .build());
              });
      return BigQueryReadClient.create(clientSettings.build());
    } catch (IOException e) {
      throw new UncheckedIOException("Error creating BigQueryStorageReadClient", e);
    }
  }

  private BigQueryWriteClient createBigQueryWriteClient(Optional<String> endpoint) {
    try {
      InstantiatingGrpcChannelProvider.Builder transportBuilder = createTransportBuilder(endpoint);
      BigQueryWriteSettings.Builder clientSettings =
          BigQueryWriteSettings.newBuilder()
              .setTransportChannelProvider(transportBuilder.build())
              .setCredentialsProvider(FixedCredentialsProvider.create(getCredentials()));
      return BigQueryWriteClient.create(clientSettings.build());
    } catch (IOException e) {
      throw new BigQueryConnectorException("Error creating BigQueryWriteClient", e);
    }
  }

  private InstantiatingGrpcChannelProvider.Builder createTransportBuilder(
      Optional<String> endpoint) {

    InstantiatingGrpcChannelProvider.Builder transportBuilder =
        BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
            .setHeaderProvider(headerProvider);
    setProxyConfig(transportBuilder);
    endpoint.ifPresent(
        e -> {
          log.info("Overriding endpoint to: ", e);
          transportBuilder.setEndpoint(e);
        });
    return transportBuilder;
  }

  private void setProxyConfig(InstantiatingGrpcChannelProvider.Builder transportBuilder) {
    BigQueryProxyConfig proxyConfig = bqConfig.getBigQueryProxyConfig();
    if (proxyConfig.getProxyUri().isPresent()) {
      transportBuilder.setChannelConfigurator(
          BigQueryProxyTransporterBuilder.createGrpcChannelConfigurator(
              proxyConfig.getProxyUri(),
              proxyConfig.getProxyUsername(),
              proxyConfig.getProxyPassword()));
    }
  }

  private void shutdownBigQueryReadClient(BigQueryReadClient bigQueryReadClient) {
    if (bigQueryReadClient != null && !bigQueryReadClient.isShutdown()) {
      bigQueryReadClient.shutdown();
    }
  }

  private void shutdownBigQueryWriteClient(BigQueryWriteClient bigQueryWriteClient) {
    if (bigQueryWriteClient != null && !bigQueryWriteClient.isShutdown()) {
      bigQueryWriteClient.shutdown();
    }
  }
}
