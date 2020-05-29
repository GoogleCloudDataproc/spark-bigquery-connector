/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import com.google.api.client.util.Base64;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.firstPresent;

public class BigQueryCredentialsSupplier {
    private final Optional<String> accessToken;
    private final Optional<String> credentialsKey;
    private final Optional<String> credentialsFile;
    private final Credentials credentials;

    public BigQueryCredentialsSupplier(
            Optional<String> accessToken,
            Optional<String> credentialsKey,
            Optional<String> credentialsFile) {
        this.accessToken = accessToken;
        this.credentialsKey = credentialsKey;
        this.credentialsFile = credentialsFile;
        // lazy creation, cache once it's created
        Optional<Credentials> credentialsFromAccessToken = credentialsKey.map(BigQueryCredentialsSupplier::createCredentialsFromAccessToken);
        Optional<Credentials> credentialsFromKey = credentialsKey.map(BigQueryCredentialsSupplier::createCredentialsFromKey);
        Optional<Credentials> credentialsFromFile = credentialsFile.map(BigQueryCredentialsSupplier::createCredentialsFromFile);
        this.credentials = firstPresent(credentialsFromAccessToken, credentialsFromKey, credentialsFromFile)
                .orElse(createDefaultCredentials());
    }

    private static Credentials createCredentialsFromAccessToken(String accessToken) {
        return GoogleCredentials.create(new AccessToken(accessToken, null));
    }

    private static Credentials createCredentialsFromKey(String key) {
        try {
            return GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.decodeBase64(key)));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create Credentials from key", e);
        }
    }

    private static Credentials createCredentialsFromFile(String file) {
        try {
            return GoogleCredentials.fromStream(new FileInputStream(file));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create Credentials from file", e);
        }
    }

    private static Credentials createDefaultCredentials() {
        try {
            return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create default Credentials", e);
        }
    }

    Credentials getCredentials() {
        return credentials;
    }
}
