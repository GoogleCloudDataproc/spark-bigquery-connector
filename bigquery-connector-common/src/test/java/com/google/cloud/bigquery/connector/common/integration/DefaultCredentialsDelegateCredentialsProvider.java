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

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.connector.common.AccessTokenProvider;
import com.google.cloud.bigquery.connector.common.AccessTokenProviderCredentials;
import java.io.IOException;

/**
 * Basic implementation of CredentialsProvider. Token TTL is very small to allow refresh testing.
 */
public class DefaultCredentialsDelegateCredentialsProvider implements CredentialsProvider {

  @Override
  public Credentials getCredentials() throws IOException {
    return new DefaultCredentialsDelegateCredentials(
        new DefaultCredentialsDelegateAccessTokenProvider());
  }
}

class DefaultCredentialsDelegateCredentials extends AccessTokenProviderCredentials {
  DefaultCredentialsDelegateAccessTokenProvider accessTokenProviderForTest;

  public DefaultCredentialsDelegateCredentials(AccessTokenProvider accessTokenProvider) {
    super(accessTokenProvider);
    this.accessTokenProviderForTest =
        (DefaultCredentialsDelegateAccessTokenProvider) accessTokenProvider;
  }

  int getCallCount() {
    return accessTokenProviderForTest.getCallCount();
  }
}
