package com.google.cloud.bigquery.connector.common;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Objects;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Optional;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityTokenSupplier implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(IdentityTokenSupplier.class);
  private static final String METADATA_VM_IDENTITY_URL =
      "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience=%s&format=%s&licenses=%s";
  private static final String METADATA_FLAVOR_HEADER_KEY = "Metadata-Flavor";
  private static final String METADATA_FLAVOR_HEADER_VALUE = "Google";
  private static final long ttlInMillis = 50 * 60 * 1000;
  private static final RequestConfig config =
      RequestConfig.custom()
          .setConnectTimeout(100)
          .setConnectionRequestTimeout(100)
          .setSocketTimeout(100)
          .build();
  private final String audience;
  private static final String format = "full";
  private static final boolean license = true;
  private long lastFetchTime;
  private String cachedToken;

  public IdentityTokenSupplier(String audience) {
    this.audience = audience;
    cachedToken = null;
    lastFetchTime = 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(cachedToken, lastFetchTime);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IdentityTokenSupplier)) {
      return false;
    }
    IdentityTokenSupplier that = (IdentityTokenSupplier) o;
    return this.cachedToken.equals(that.cachedToken) && this.lastFetchTime == that.lastFetchTime;
  }

  public Optional<String> getIdentityToken() {
    long currentTimeInMillis = System.currentTimeMillis();
    if (cachedToken == null || currentTimeInMillis - lastFetchTime >= ttlInMillis) {
      CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();
      HttpGet httpGet =
          new HttpGet(String.format(METADATA_VM_IDENTITY_URL, audience, format, license));
      httpGet.addHeader(METADATA_FLAVOR_HEADER_KEY, METADATA_FLAVOR_HEADER_VALUE);
      try {
        CloseableHttpResponse response = httpClient.execute(httpGet);
        if (response.getStatusLine().getStatusCode() == 200) {
          cachedToken =
              CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), UTF_8));
        } else {
          cachedToken = null;
          log.info(
              "Unable to obtain identity token, response code: [{}]",
              response.getStatusLine().getStatusCode());
        }
      } catch (IOException ex) {
        log.info("Unable to obtain identity token", ex);
        cachedToken = null;
      } finally {
        try {
          Closeables.close(httpClient, true);
        } catch (IOException e) {
          // nothing to do
        }
      }
      lastFetchTime = currentTimeInMillis;
    }
    return Optional.ofNullable(cachedToken);
  }
}
