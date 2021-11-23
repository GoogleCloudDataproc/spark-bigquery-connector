package com.google.cloud.bigquery.connector.common;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1beta2.BigQueryWriteClient;
import java.net.URI;
import java.util.Optional;
import org.junit.Test;

public class BigQueryClientFactoryTest {
  BigQueryCredentialsSupplier bigQueryCredentialsSupplier = mock(BigQueryCredentialsSupplier.class);
  UserAgentHeaderProvider userAgentHeaderProvider = mock(UserAgentHeaderProvider.class);
  BigQueryConfig bigQueryConfig = mock(BigQueryConfig.class);
  BigQueryProxyConfig bigQueryProxyConfig =
      new BigQueryProxyConfig() {
        @Override
        public Optional<URI> getProxyUri() {
          return Optional.empty();
        }

        @Override
        public Optional<String> getProxyUsername() {
          return Optional.empty();
        }

        @Override
        public Optional<String> getProxyPassword() {
          return Optional.empty();
        }
      };

  @Test
  public void testGetBigQueryReadClientWithEmptyOptionalEndpoint() {
    BigQueryClientFactory clientFactory =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryReadClient readClient = clientFactory.getBigQueryReadClient(Optional.empty());
    assertNotNull(readClient);

    BigQueryReadClient readClient2 = clientFactory.getBigQueryReadClient(Optional.empty());
    assertNotNull(readClient2);

    assertSame(readClient, readClient2);
  }

  @Test
  public void testGetBigQueryReadClientWithDifferentEndpoints() {
    BigQueryClientFactory clientFactory =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryReadClient usReadClient = clientFactory.getBigQueryReadClient(Optional.of("US:8080"));
    assertNotNull(usReadClient);

    BigQueryReadClient usReadClient2 = clientFactory.getBigQueryReadClient(Optional.of("US:8080"));
    assertNotNull(usReadClient2);
    assertSame(usReadClient, usReadClient2);

    BigQueryReadClient euReadClient = clientFactory.getBigQueryReadClient(Optional.of("EU:8080"));
    assertNotNull(euReadClient);

    BigQueryReadClient euReadClient2 = clientFactory.getBigQueryReadClient(Optional.of("EU:8080"));
    assertNotNull(euReadClient2);
    assertSame(euReadClient, euReadClient2);

    assertNotSame(usReadClient, euReadClient);
  }

  @Test
  public void testGetBigQueryWriteClient() {
    BigQueryClientFactory clientFactory =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryWriteClient writeClient = clientFactory.getBigQueryWriteClient();
    assertNotNull(writeClient);

    BigQueryWriteClient writeClient2 = clientFactory.getBigQueryWriteClient();
    assertNotNull(writeClient2);
    assertSame(writeClient, writeClient2);
  }
}
