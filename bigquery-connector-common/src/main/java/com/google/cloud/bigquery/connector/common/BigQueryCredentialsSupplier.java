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

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.createVerifiedInstance;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.verifySerialization;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.util.Base64;
import com.google.auth.Credentials;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryCredentialsSupplier {
  private final Credentials credentials;
  public static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  private static final Logger log = LoggerFactory.getLogger(BigQueryCredentialsSupplier.class);

  public BigQueryCredentialsSupplier(
      Optional<String> accessTokenProviderFQCN,
      Optional<String> accessTokenProviderConfig,
      Optional<String> accessToken,
      Optional<String> credentialsKey,
      Optional<String> credentialsFile,
      String loggedInUserName,
      Set<String> loggedInUserGroups,
      Optional<Map<String, String>> impersonationServiceAccountsForUsers,
      Optional<Map<String, String>> impersonationServiceAccountsForGroups,
      Optional<String> impersonationServiceAccount,
      Optional<ImmutableList<String>> credentialsScopes,
      Optional<URI> proxyUri,
      Optional<String> proxyUsername,
      Optional<String> proxyPassword) {
    GoogleCredentials credentials;
    if (accessTokenProviderFQCN.isPresent()) {
      AccessTokenProvider accessTokenProvider =
          accessTokenProviderConfig
              .map(
                  config ->
                      createVerifiedInstance(
                          accessTokenProviderFQCN.get(), AccessTokenProvider.class, config))
              .orElseGet(
                  () ->
                      createVerifiedInstance(
                          accessTokenProviderFQCN.get(), AccessTokenProvider.class));
      credentials = new AccessTokenProviderCredentials(verifySerialization(accessTokenProvider));
    } else if (accessToken.isPresent()) {
      credentials = createCredentialsFromAccessToken(accessToken.get());
    } else if (credentialsKey.isPresent()) {
      credentials =
          createCredentialsFromKey(credentialsKey.get(), proxyUri, proxyUsername, proxyPassword);
    } else if (credentialsFile.isPresent()) {
      credentials =
          createCredentialsFromFile(credentialsFile.get(), proxyUri, proxyUsername, proxyPassword);
    } else {
      credentials = createDefaultCredentials();
    }
    Optional<GoogleCredentials> impersonatedCredentials =
        createCredentialsFromImpersonation(
            loggedInUserName,
            loggedInUserGroups,
            impersonationServiceAccountsForUsers,
            impersonationServiceAccountsForGroups,
            impersonationServiceAccount,
            (GoogleCredentials) credentials,
            proxyUri,
            proxyUsername,
            proxyPassword);
    credentials = impersonatedCredentials.orElse(credentials);
    this.credentials = credentialsScopes.map(credentials::createScoped).orElse(credentials);
  }

  private static GoogleCredentials createCredentialsFromAccessToken(String accessToken) {
    return GoogleCredentials.create(new AccessToken(accessToken, null));
  }

  private static Optional<GoogleCredentials> createCredentialsFromImpersonation(
      String loggedInUserName,
      Set<String> loggedInUserGroups,
      Optional<Map<String, String>> impersonationServiceAccountsForUsers,
      Optional<Map<String, String>> impersonationServiceAccountsForGroups,
      Optional<String> impersonationServiceAccount,
      GoogleCredentials sourceCredentials,
      Optional<URI> proxyUri,
      Optional<String> proxyUsername,
      Optional<String> proxyPassword) {

    Optional<String> serviceAccountToImpersonate =
        Stream.of(
                () ->
                    getServiceAccountToImpersonateByKeys(
                        impersonationServiceAccountsForUsers,
                        Optional.ofNullable(loggedInUserName)
                            .map(ImmutableList::of)
                            .orElseGet(ImmutableList::of)),
                () ->
                    getServiceAccountToImpersonateByKeys(
                        impersonationServiceAccountsForGroups,
                        Optional.ofNullable(loggedInUserGroups).orElse(new HashSet<>())),
                (Supplier<Optional<String>>) () -> impersonationServiceAccount)
            .map(Supplier::get)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(sa -> !isNullOrEmpty(sa))
            .findFirst();

    HttpTransportFactory httpTransportFactory =
        BigQueryProxyTransporterBuilder.createHttpTransportFactory(
            proxyUri, proxyUsername, proxyPassword);
    return serviceAccountToImpersonate.map(
        sa ->
            ImpersonatedCredentials.newBuilder()
                .setSourceCredentials(sourceCredentials)
                .setTargetPrincipal(serviceAccountToImpersonate.get())
                .setScopes(ImmutableList.of(CLOUD_PLATFORM_SCOPE))
                .setHttpTransportFactory(httpTransportFactory)
                .build());
  }

  private static Optional<String> getServiceAccountToImpersonateByKeys(
      Optional<Map<String, String>> serviceAccountMapping, Collection<String> keys) {
    return serviceAccountMapping.flatMap(
        mapping ->
            mapping.entrySet().stream()
                .filter(e -> keys.contains(e.getKey()))
                .map(Map.Entry::getValue)
                .findFirst());
  }

  private static GoogleCredentials createCredentialsFromKey(
      String key,
      Optional<URI> proxyUri,
      Optional<String> proxyUsername,
      Optional<String> proxyPassword) {
    try {
      if (proxyUri.isPresent()) {
        HttpTransportFactory httpTransportFactory =
            BigQueryProxyTransporterBuilder.createHttpTransportFactory(
                proxyUri, proxyUsername, proxyPassword);
        return GoogleCredentials.fromStream(
            new ByteArrayInputStream(Base64.decodeBase64(key)), httpTransportFactory);
      } else {
        return GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.decodeBase64(key)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create Credentials from key", e);
    }
  }

  private static GoogleCredentials createCredentialsFromFile(
      String file,
      Optional<URI> proxyUri,
      Optional<String> proxyUsername,
      Optional<String> proxyPassword) {
    try {
      if (proxyUri.isPresent()) {
        HttpTransportFactory httpTransportFactory =
            BigQueryProxyTransporterBuilder.createHttpTransportFactory(
                proxyUri, proxyUsername, proxyPassword);
        return GoogleCredentials.fromStream(new FileInputStream(file), httpTransportFactory);
      } else {
        return GoogleCredentials.fromStream(new FileInputStream(file));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create Credentials from file", e);
    }
  }

  public static GoogleCredentials createDefaultCredentials() {
    try {
      return GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create default Credentials", e);
    }
  }

  public Credentials getCredentials() {
    return credentials;
  }

  public String getUniverseDomain() {
    String universeDomain = Credentials.GOOGLE_DEFAULT_UNIVERSE;
    try {
      universeDomain = getCredentials().getUniverseDomain();
    } catch (IOException e) {
      log.warn(
          "Caught Exception while querying the Universe Domain, continuing with GOOGLE_DEFAULT_UNIVERSE");
    }
    return universeDomain;
  }
}
