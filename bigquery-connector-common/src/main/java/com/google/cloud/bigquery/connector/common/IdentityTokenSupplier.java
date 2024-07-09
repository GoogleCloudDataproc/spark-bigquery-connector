package com.google.cloud.bigquery.connector.common;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
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
  private static final RequestConfig config =
      RequestConfig.custom()
          .setConnectTimeout(100)
          .setConnectionRequestTimeout(100)
          .setSocketTimeout(100)
          .build();
  private static final String format = "full";
  private static final boolean license = true;

  public static String getIdentityToken(String audience) {
    CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();
    String token = null;
    ;
    HttpGet httpGet =
        new HttpGet(String.format(METADATA_VM_IDENTITY_URL, audience, format, license));
    httpGet.addHeader(METADATA_FLAVOR_HEADER_KEY, METADATA_FLAVOR_HEADER_VALUE);
    try {
      CloseableHttpResponse response = httpClient.execute(httpGet);
      if (response.getStatusLine().getStatusCode() == 200) {
        token =
            CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), UTF_8));
      } else {
        log.info(
            "Unable to obtain identity token, response code: [{}]",
            response.getStatusLine().getStatusCode());
      }
    } catch (IOException ex) {
      log.info("Unable to obtain identity token", ex);
    } finally {
      try {
        Closeables.close(httpClient, true);
      } catch (IOException e) {
        // nothing to do
      }
    }
    return token;
  }
}
