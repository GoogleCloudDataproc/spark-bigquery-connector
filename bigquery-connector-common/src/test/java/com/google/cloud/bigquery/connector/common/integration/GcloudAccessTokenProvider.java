/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigquery.connector.common.integration;

import static com.google.common.truth.Truth.assertThat;

import com.google.auth.oauth2.AccessToken;
import com.google.cloud.bigquery.connector.common.AccessTokenProvider;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * Basic implementation of AccessTokenProvider. Token TTL is very small to allow refresh testing.
 */
public class GcloudAccessTokenProvider implements AccessTokenProvider {

  private int callCount = 0;

  @Override
  public AccessToken getAccessToken() throws IOException {
    try {
      Process gcloud = new ProcessBuilder("gcloud", "auth", "print-access-token").start();
      assertThat(gcloud.waitFor()).isEqualTo(0);
      String token =
          CharStreams.toString(
                  new InputStreamReader(gcloud.getInputStream(), StandardCharsets.UTF_8))
              .trim();
      callCount++;
      return new AccessToken(token, new Date(System.currentTimeMillis() + 2000));
    } catch (InterruptedException e) {
      throw new IOException("Failed to get token", e);
    }
  }

  int getCallCount() {
    return callCount;
  }
}
