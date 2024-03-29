package com.google.cloud.bigquery.connector.common;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityTokenSupplier implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(IdentityTokenSupplier.class);
  private static final String METADATA_VM_IDENTITY_URL =
      "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience=%s&format=%s&licenses=%s";
  private static final String METADATA_FLAVOR_HEADER_KEY = "Metadata-Flavor";
  private static final String METADATA_FLAVOR_HEADER_VALUE = "Google";
  private static final long ttlInMillis = 50 * 60 * 1000;
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

  public Optional<String> getIdentityToken() {
    long currentTimeInMillis = System.currentTimeMillis();
    if (cachedToken != null || currentTimeInMillis - lastFetchTime >= ttlInMillis) {
      try {
        URL url = new URL(String.format(METADATA_VM_IDENTITY_URL, audience, format, license));

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestProperty(METADATA_FLAVOR_HEADER_KEY, METADATA_FLAVOR_HEADER_VALUE);
        connection.setRequestMethod("GET");

        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
          cachedToken =
              CharStreams.toString(
                  new InputStreamReader(connection.getInputStream(), Charsets.UTF_8));
        } else {
          cachedToken = null;
          log.info("Unable to obtain identity token, response code: [{}]", responseCode);
        }
      } catch (IOException ex) {
        log.info("Unable to obtain identity token", ex);
        cachedToken = null;
      }
      lastFetchTime = currentTimeInMillis;
    }
    return Optional.ofNullable(cachedToken);
  }
}
