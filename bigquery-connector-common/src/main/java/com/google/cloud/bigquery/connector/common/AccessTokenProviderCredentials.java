package com.google.cloud.bigquery.connector.common;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;

public class AccessTokenProviderCredentials extends GoogleCredentials {

  private final AccessTokenProvider accessTokenProvider;

  public AccessTokenProviderCredentials(AccessTokenProvider accessTokenProvider) {
    this.accessTokenProvider = accessTokenProvider;
  }

  @Override
  public AccessToken refreshAccessToken() throws IOException {
    return accessTokenProvider.getAccessToken();
  }
}
