/*
 * Copyright 2026 Google Inc. All Rights Reserved.
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

import com.google.auth.oauth2.GoogleCredentials;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * An {@link AccessTokenProvider} implementation that loads credentials dynamically from a local
 * file path (such as a Workload Identity Federation or Service Account JSON key file).
 *
 * <p>This provider is fully serializable across Spark Executors because it only stores the
 * credentials file path as a String. Credentials parsing and token refreshes occur dynamically on
 * demand inside {@link #getAccessToken()}.
 */
public class FileCredentialsAccessTokenProvider implements AccessTokenProvider {

  private static final long serialVersionUID = 1L;
  private final String credentialsPath;

  public FileCredentialsAccessTokenProvider() {
    this(null);
  }

  /**
   * Constructs the provider using the specified file path.
   *
   * @param credentialsPath Path to the credentials file passed via gcpAccessTokenProviderConfig.
   */
  public FileCredentialsAccessTokenProvider(String credentialsPath) {
    this.credentialsPath = credentialsPath;
  }

  @Override
  public AccessToken getAccessToken() throws IOException {
    if (credentialsPath == null || credentialsPath.trim().isEmpty()) {
      throw new IllegalArgumentException("credentialsPath must not be null or empty");
    }
    try (FileInputStream fis = new FileInputStream(credentialsPath)) {
      GoogleCredentials credentials = GoogleCredentials.fromStream(fis);
      credentials.refreshIfExpired();

      com.google.auth.oauth2.AccessToken token = credentials.getAccessToken();
      if (token == null) {
        credentials.refresh();
        token = credentials.getAccessToken();
      }
      return new AccessToken(token.getTokenValue(), token.getExpirationTime());
    }
  }

  public String getCredentialsPath() {
    return credentialsPath;
  }
}
