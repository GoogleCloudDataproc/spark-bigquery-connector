package com.google.cloud.bigquery.connector.common;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import com.google.auth.oauth2.IdTokenProvider.Option;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityTokenSupplier implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(IdentityTokenSupplier.class);
  private static final Option FORMAT = Option.FORMAT_FULL;
  private static final Option LICENSE = Option.LICENSES_TRUE;

  public static Optional<String> fetchIdentityToken(String audience) {
    try {
      GoogleCredentials googleCredentials = GoogleCredentials.getApplicationDefault();

      IdTokenCredentials idTokenCredentials =
          IdTokenCredentials.newBuilder()
              .setIdTokenProvider((IdTokenProvider) googleCredentials)
              .setTargetAudience(audience)
              .setOptions(Arrays.asList(FORMAT, LICENSE))
              .build();

      return Optional.ofNullable(idTokenCredentials.refreshAccessToken().getTokenValue());
    } catch (IOException ex) {
      log.info("Unable to obtain identity token", ex);
    }
    return Optional.empty();
  }
}
