package com.google.cloud.bigquery.connector.common;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1beta2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1beta2.BigQueryWriteSettings;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Optional;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Since Guice recommends to avoid injecting closeable resources (see
 * https://github.com/google/guice/wiki/Avoid-Injecting-Closable-Resources), this factory creates
 * short lived clients that can be closed independently.
 */
public class BigQueryClientFactory implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(BigQueryClientFactory.class);

  private final Credentials credentials;
  // using the user agent as HeaderProvider is not serializable
  private final UserAgentHeaderProvider userAgentHeaderProvider;
  private final BigQueryConfig bqConfig;

  @Inject
  public BigQueryClientFactory(
      BigQueryCredentialsSupplier bigQueryCredentialsSupplier,
      UserAgentHeaderProvider userAgentHeaderProvider,
      BigQueryConfig bqConfig) {
    // using Guava's optional as it is serializable
    this.credentials = bigQueryCredentialsSupplier.getCredentials();
    this.userAgentHeaderProvider = userAgentHeaderProvider;
    this.bqConfig = bqConfig;
  }

  public BigQueryReadClient createBigQueryReadClient(Optional<String> endpoint) {
    try {
      InstantiatingGrpcChannelProvider.Builder transportBuilder =
          BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
              .setHeaderProvider(userAgentHeaderProvider);
      setProxyConfig(transportBuilder);
      endpoint.ifPresent(
          e -> {
            log.info("Overriding endpoint to: ", e);
            transportBuilder.setEndpoint(e);
          });
      BigQueryReadSettings.Builder clientSettings =
          BigQueryReadSettings.newBuilder()
              .setTransportChannelProvider(transportBuilder.build())
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials));
      return BigQueryReadClient.create(clientSettings.build());
    } catch (IOException e) {
      throw new UncheckedIOException("Error creating BigQueryStorageReadClient", e);
    }
  }

  public BigQueryWriteClient createBigQueryWriteClient() {
    try {
      InstantiatingGrpcChannelProvider.Builder transportBuilder =
          BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
              .setHeaderProvider(userAgentHeaderProvider);
      setProxyConfig(transportBuilder);
      BigQueryWriteSettings.Builder clientSettings =
          BigQueryWriteSettings.newBuilder()
              .setTransportChannelProvider(transportBuilder.build())
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials));
      return BigQueryWriteClient.create(clientSettings.build());
    } catch (IOException e) {
      throw new BigQueryConnectorException("Error creating BigQueryWriteClient", e);
    }
  }

  public void setProxyConfig(InstantiatingGrpcChannelProvider.Builder transportBuilder) {
    BigQueryProxyConfig proxyConfig = bqConfig.getBigQueryProxyConfig();
    if (proxyConfig.getProxyUri().isPresent()) {
      transportBuilder.setChannelConfigurator(
          BigQueryProxyTransporterBuilder.createGrpcChannelConfigurator(
              proxyConfig.getProxyUri(),
              proxyConfig.getProxyUsername(),
              proxyConfig.getProxyPassword()));
    }
  }
}
