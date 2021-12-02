package com.google.cloud.bigquery.connector.common;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1beta2.BigQueryWriteClient;
import com.google.common.base.Objects;
import java.net.URI;
import java.security.PrivateKey;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.junit.Test;

public class BigQueryClientFactoryTest {
  private static final String CLIENT_EMAIL =
      "36680232662-vrd7ji19qe3nelgchd0ah2csanun6bnr@developer.gserviceaccount.com";
  private static final String PRIVATE_KEY_ID = "d84a4fefcf50791d4a90f2d7af17469d6282df9d";
  private static final Collection<String> SCOPES = Collections.singletonList("dummy.scope");
  private static final String USER = "user@example.com";
  private static final String PROJECT_ID = "project-id";

  private final PrivateKey privateKey = mock(PrivateKey.class);
  private final BigQueryCredentialsSupplier bigQueryCredentialsSupplier =
      mock(BigQueryCredentialsSupplier.class);
  private final UserAgentHeaderProvider userAgentHeaderProvider =
      new UserAgentHeaderProvider("test-agent");
  private final BigQueryConfig bigQueryConfig = mock(BigQueryConfig.class);
  private final BigQueryProxyConfig bigQueryProxyConfig =
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
  public void testGetReadClientForSameClientFactory() {
    BigQueryClientFactory clientFactory =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryReadClient readClient = clientFactory.getBigQueryReadClient();
    assertNotNull(readClient);

    BigQueryReadClient readClient2 = clientFactory.getBigQueryReadClient();
    assertNotNull(readClient2);

    assertSame(readClient, readClient2);
  }

  @Test
  public void testGetReadClientWithUserAgent() {
    BigQueryClientFactory clientFactory =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryReadClient readClient = clientFactory.getBigQueryReadClient();
    assertNotNull(readClient);

    BigQueryClientFactory clientFactory2 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, new UserAgentHeaderProvider("test-agent"), bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryReadClient readClient2 = clientFactory2.getBigQueryReadClient();
    assertNotNull(readClient2);

    assertSame(readClient, readClient2);

    BigQueryClientFactory clientFactory3 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier,
            new UserAgentHeaderProvider("test-agent-2"),
            bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryReadClient readClient3 = clientFactory3.getBigQueryReadClient();
    assertNotNull(readClient3);

    assertNotSame(readClient, readClient3);
    assertNotSame(readClient2, readClient3);
  }

  @Test
  public void testGetReadClientWithBigQueryConfig() {
    BigQueryClientFactory clientFactory =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier,
            userAgentHeaderProvider,
            new TestBigQueryConfig(Optional.of("US:8080")));

    BigQueryReadClient readClient = clientFactory.getBigQueryReadClient();
    assertNotNull(readClient);

    BigQueryClientFactory clientFactory2 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier,
            userAgentHeaderProvider,
            new TestBigQueryConfig(Optional.of("US:8080")));

    BigQueryReadClient readClient2 = clientFactory2.getBigQueryReadClient();
    assertNotNull(readClient2);

    assertSame(readClient, readClient2);

    BigQueryClientFactory clientFactory3 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier,
            userAgentHeaderProvider,
            new TestBigQueryConfig(Optional.of("EU:8080")));

    BigQueryReadClient readClient3 = clientFactory3.getBigQueryReadClient();
    assertNotNull(readClient3);

    assertNotSame(readClient, readClient3);
    assertNotSame(readClient2, readClient3);
  }

  @Test
  public void testGetReadClientWithServiceAccountCredentials() {
    when(bigQueryCredentialsSupplier.getCredentials())
        .thenReturn(createServiceAccountCredentials("test-client-id"));
    BigQueryClientFactory clientFactory =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryReadClient readClient = clientFactory.getBigQueryReadClient();
    assertNotNull(readClient);

    when(bigQueryCredentialsSupplier.getCredentials())
        .thenReturn(createServiceAccountCredentials("test-client-id"));
    BigQueryClientFactory clientFactory2 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryReadClient readClient2 = clientFactory2.getBigQueryReadClient();
    assertNotNull(readClient2);

    assertSame(readClient, readClient2);

    when(bigQueryCredentialsSupplier.getCredentials())
        .thenReturn(createServiceAccountCredentials("test-client-id-2"));
    BigQueryClientFactory clientFactory3 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryReadClient readClient3 = clientFactory3.getBigQueryReadClient();
    assertNotNull(readClient3);

    assertNotSame(readClient, readClient3);
    assertNotSame(readClient2, readClient3);
  }

  @Test
  public void testGetWriteClientForSameClientFactory() {
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

  @Test
  public void testGetWriteClientWithUserAgent() {
    BigQueryClientFactory clientFactory =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryWriteClient writeClient = clientFactory.getBigQueryWriteClient();
    assertNotNull(writeClient);

    BigQueryClientFactory clientFactory2 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, new UserAgentHeaderProvider("test-agent"), bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryWriteClient writeClient2 = clientFactory2.getBigQueryWriteClient();
    assertNotNull(writeClient2);

    assertSame(writeClient, writeClient2);

    BigQueryClientFactory clientFactory3 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier,
            new UserAgentHeaderProvider("test-agent-2"),
            bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryWriteClient writeClient3 = clientFactory3.getBigQueryWriteClient();
    assertNotNull(writeClient3);

    assertNotSame(writeClient, writeClient3);
    assertNotSame(writeClient2, writeClient3);
  }

  @Test
  public void testGetWriteClientWithBigQueryConfig() {
    BigQueryClientFactory clientFactory =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier,
            userAgentHeaderProvider,
            new TestBigQueryConfig(Optional.of("US:8080")));

    BigQueryWriteClient writeClient = clientFactory.getBigQueryWriteClient();
    assertNotNull(writeClient);

    BigQueryClientFactory clientFactory2 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier,
            userAgentHeaderProvider,
            new TestBigQueryConfig(Optional.of("US:8080")));

    BigQueryWriteClient writeClient2 = clientFactory2.getBigQueryWriteClient();
    assertNotNull(writeClient2);

    assertSame(writeClient, writeClient2);

    BigQueryClientFactory clientFactory3 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier,
            userAgentHeaderProvider,
            new TestBigQueryConfig(Optional.of("EU:8080")));

    BigQueryWriteClient writeClient3 = clientFactory3.getBigQueryWriteClient();
    assertNotNull(writeClient3);

    assertNotSame(writeClient, writeClient3);
    assertNotSame(writeClient2, writeClient3);
  }

  @Test
  public void testGetWriteClientWithServiceAccountCredentials() throws Exception {
    when(bigQueryCredentialsSupplier.getCredentials())
        .thenReturn(createServiceAccountCredentials("test-client-id"));
    BigQueryClientFactory clientFactory =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryWriteClient writeClient = clientFactory.getBigQueryWriteClient();
    assertNotNull(writeClient);

    when(bigQueryCredentialsSupplier.getCredentials())
        .thenReturn(createServiceAccountCredentials("test-client-id"));
    BigQueryClientFactory clientFactory2 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryWriteClient writeClient2 = clientFactory2.getBigQueryWriteClient();
    assertNotNull(writeClient2);

    assertSame(writeClient, writeClient2);

    when(bigQueryCredentialsSupplier.getCredentials())
        .thenReturn(createServiceAccountCredentials("test-client-id-2"));
    BigQueryClientFactory clientFactory3 =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, bigQueryConfig);

    when(bigQueryConfig.getBigQueryProxyConfig()).thenReturn(bigQueryProxyConfig);

    BigQueryWriteClient writeClient3 = clientFactory3.getBigQueryWriteClient();
    assertNotNull(writeClient3);

    assertNotSame(writeClient, writeClient3);
    assertNotSame(writeClient2, writeClient3);
  }

  private ServiceAccountCredentials createServiceAccountCredentials(String clientId) {
    return ServiceAccountCredentials.newBuilder()
        .setClientId(clientId)
        .setClientEmail(CLIENT_EMAIL)
        .setPrivateKey(privateKey)
        .setPrivateKeyId(PRIVATE_KEY_ID)
        .setScopes(SCOPES)
        .setServiceAccountUser(USER)
        .setProjectId(PROJECT_ID)
        .build();
  }

  private class TestBigQueryConfig implements BigQueryConfig {

    private final Optional<String> endpoint;

    TestBigQueryConfig(Optional<String> endpoint) {
      this.endpoint = endpoint;
    }

    @Override
    public Optional<String> getCredentialsKey() {
      return Optional.empty();
    }

    @Override
    public Optional<String> getCredentialsFile() {
      return Optional.empty();
    }

    @Override
    public Optional<String> getAccessToken() {
      return Optional.empty();
    }

    @Override
    public String getParentProjectId() {
      return null;
    }

    @Override
    public boolean isViewsEnabled() {
      return false;
    }

    @Override
    public Optional<String> getMaterializationProject() {
      return Optional.empty();
    }

    @Override
    public Optional<String> getMaterializationDataset() {
      return Optional.empty();
    }

    @Override
    public int getBigQueryClientConnectTimeout() {
      return 0;
    }

    @Override
    public int getBigQueryClientReadTimeout() {
      return 0;
    }

    @Override
    public RetrySettings getBigQueryClientRetrySettings() {
      return null;
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
      if (!(o instanceof TestBigQueryConfig)) {
        return false;
      }
      TestBigQueryConfig that = (TestBigQueryConfig) o;
      return Objects.equal(endpoint, that.endpoint);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(endpoint);
    }
  }
}
