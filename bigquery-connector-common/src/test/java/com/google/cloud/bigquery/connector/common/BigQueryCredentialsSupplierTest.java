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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.gson.GsonFactory;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.gson.stream.MalformedJsonException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import org.junit.Test;

public class BigQueryCredentialsSupplierTest {

  // Taken from
  // https://github.com/googleapis/google-auth-library-java/blob/a329c4171735c3d4ee574978e6c3742b96c01f74/oauth2_http/javatests/com/google/auth/oauth2/ServiceAccountCredentialsTest.java
  // As this code is private it cannot be directly used.

  static final String PRIVATE_KEY_PKCS8 =
      "-----BEGIN PRIVATE KEY-----\n"
          + "MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBALX0PQoe1igW12i"
          + "kv1bN/r9lN749y2ijmbc/mFHPyS3hNTyOCjDvBbXYbDhQJzWVUikh4mvGBA07qTj79Xc3yBDfKP2IeyYQIFe0t0"
          + "zkd7R9Zdn98Y2rIQC47aAbDfubtkU1U72t4zL11kHvoa0/RuFZjncvlr42X7be7lYh4p3NAgMBAAECgYASk5wDw"
          + "4Az2ZkmeuN6Fk/y9H+Lcb2pskJIXjrL533vrDWGOC48LrsThMQPv8cxBky8HFSEklPpkfTF95tpD43iVwJRB/Gr"
          + "CtGTw65IfJ4/tI09h6zGc4yqvIo1cHX/LQ+SxKLGyir/dQM925rGt/VojxY5ryJR7GLbCzxPnJm/oQJBANwOCO6"
          + "D2hy1LQYJhXh7O+RLtA/tSnT1xyMQsGT+uUCMiKS2bSKx2wxo9k7h3OegNJIu1q6nZ6AbxDK8H3+d0dUCQQDTrP"
          + "SXagBxzp8PecbaCHjzNRSQE2in81qYnrAFNB4o3DpHyMMY6s5ALLeHKscEWnqP8Ur6X4PvzZecCWU9BKAZAkAut"
          + "LPknAuxSCsUOvUfS1i87ex77Ot+w6POp34pEX+UWb+u5iFn2cQacDTHLV1LtE80L8jVLSbrbrlH43H0DjU5AkEA"
          + "gidhycxS86dxpEljnOMCw8CKoUBd5I880IUahEiUltk7OLJYS/Ts1wbn3kPOVX3wyJs8WBDtBkFrDHW2ezth2QJ"
          + "ADj3e1YhMVdjJW5jqwlD/VNddGjgzyunmiZg0uOXsHXbytYmsA545S8KRQFaJKFXYYFo2kOjqOiC1T2cAzMDjCQ"
          + "==\n-----END PRIVATE KEY-----\n";
  private static final String TYPE = "service_account";
  private static final String CLIENT_EMAIL =
      "36680232662-vrd7ji19qe3nelgchd0ah2csanun6bnr@developer.gserviceaccount.com";
  private static final String CLIENT_ID =
      "36680232662-vrd7ji19qe3nelgchd0ah2csanun6bnr.apps.googleusercontent.com";
  private static final String PRIVATE_KEY_ID = "d84a4fefcf50791d4a90f2d7af17469d6282df9d";
  private static final String ACCESS_TOKEN = "1/MkSJoj1xsli0AccessToken_NKPY2";
  private static final Collection<String> SCOPES = Collections.singletonList("dummy.scope");
  private static final String QUOTA_PROJECT = "sample-quota-project-id";
  private static final Optional<URI> optionalProxyURI =
      Optional.of(URI.create("http://bq-connector-host:1234"));
  private static final Optional<String> optionalProxyUserName = Optional.of("credential-user");
  private static final Optional<String> optionalProxyPassword = Optional.of("credential-password");
  public static final String IMPERSONATED_GLOBAL =
      "impersonated-global@developer.gserviceaccount.com";
  public static final String IMPERSONATED_A = "impersonated-a@developer.gserviceaccount.com";
  public static final String IMPERSONATED_B = "impersonated-b@developer.gserviceaccount.com";

  @Test
  public void testCredentialsFromAccessToken() {
    Credentials nonProxyCredentials =
        new BigQueryCredentialsSupplier(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ACCESS_TOKEN),
                Optional.empty(),
                Optional.empty(),
                null,
                null,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty())
            .getCredentials();

    Credentials proxyCredentials =
        new BigQueryCredentialsSupplier(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ACCESS_TOKEN),
                Optional.empty(),
                Optional.empty(),
                null,
                null,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                optionalProxyURI,
                optionalProxyUserName,
                optionalProxyPassword)
            .getCredentials();

    // the output should be same for both proxyCredentials and nonProxyCredentials
    Arrays.asList(nonProxyCredentials, proxyCredentials).stream()
        .forEach(
            credentials -> {
              assertThat(credentials).isInstanceOf(GoogleCredentials.class);
              GoogleCredentials googleCredentials = (GoogleCredentials) credentials;
              assertThat(googleCredentials.getAccessToken().getTokenValue())
                  .isEqualTo(ACCESS_TOKEN);
            });
  }

  @Test
  public void testCredentialsFromKey() throws Exception {
    String json = createServiceAccountJson("key");
    String credentialsKey =
        Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));

    Credentials nonProxyCredentials =
        new BigQueryCredentialsSupplier(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(credentialsKey),
                Optional.empty(),
                null,
                null,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty())
            .getCredentials();

    Credentials proxyCredentials =
        new BigQueryCredentialsSupplier(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(credentialsKey),
                Optional.empty(),
                null,
                null,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                optionalProxyURI,
                optionalProxyUserName,
                optionalProxyPassword)
            .getCredentials();

    // the output should be same for both proxyCredentials and nonProxyCredentials
    Arrays.asList(nonProxyCredentials, proxyCredentials).stream()
        .forEach(
            credentials -> {
              assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
              ServiceAccountCredentials serviceAccountCredentials =
                  (ServiceAccountCredentials) credentials;
              assertThat(serviceAccountCredentials.getProjectId()).isEqualTo("key");
              assertThat(serviceAccountCredentials.getClientEmail()).isEqualTo(CLIENT_EMAIL);
              assertThat(serviceAccountCredentials.getClientId()).isEqualTo(CLIENT_ID);
              assertThat(serviceAccountCredentials.getQuotaProjectId()).isEqualTo(QUOTA_PROJECT);
            });
  }

  @Test
  public void testCredentialsFromKeyWithErrors() throws Exception {
    String json = createServiceAccountJson("key");
    String credentialsKey =
        Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));

    IllegalArgumentException exceptionWithPassword =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new BigQueryCredentialsSupplier(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(credentialsKey),
                        Optional.empty(),
                        null,
                        null,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        optionalProxyURI,
                        Optional.empty(),
                        optionalProxyPassword)
                    .getCredentials());

    IllegalArgumentException exceptionWithUserName =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new BigQueryCredentialsSupplier(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(credentialsKey),
                        Optional.empty(),
                        null,
                        null,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        optionalProxyURI,
                        optionalProxyUserName,
                        Optional.empty())
                    .getCredentials());

    Arrays.asList(exceptionWithPassword, exceptionWithUserName).stream()
        .forEach(
            exception -> {
              assertThat(exception)
                  .hasMessageThat()
                  .contains(
                      "Both proxyUsername and proxyPassword should be defined or not defined together");
            });
  }

  @Test
  public void testCredentialsFromFile() throws Exception {
    String json = createServiceAccountJson("file");
    File credentialsFile = File.createTempFile("dummy-credentials", ".json");
    credentialsFile.deleteOnExit();
    Files.write(credentialsFile.toPath(), json.getBytes(StandardCharsets.UTF_8));

    Credentials nonProxyCredentials =
        new BigQueryCredentialsSupplier(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(credentialsFile.getAbsolutePath()),
                null,
                null,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty())
            .getCredentials();

    Credentials proxyCredentials =
        new BigQueryCredentialsSupplier(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(credentialsFile.getAbsolutePath()),
                null,
                null,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                optionalProxyURI,
                optionalProxyUserName,
                optionalProxyPassword)
            .getCredentials();

    // the output should be same for both proxyCredentials and nonProxyCredentials
    Arrays.asList(nonProxyCredentials, proxyCredentials).stream()
        .forEach(
            credentials -> {
              assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
              ServiceAccountCredentials serviceAccountCredentials =
                  (ServiceAccountCredentials) credentials;
              assertThat(serviceAccountCredentials.getProjectId()).isEqualTo("file");
              assertThat(serviceAccountCredentials.getClientEmail()).isEqualTo(CLIENT_EMAIL);
              assertThat(serviceAccountCredentials.getClientId()).isEqualTo(CLIENT_ID);
              assertThat(serviceAccountCredentials.getQuotaProjectId()).isEqualTo(QUOTA_PROJECT);
            });
  }

  @Test
  public void testCredentialsFromFileWithErrors() throws Exception {
    String json = createServiceAccountJson("file");
    File credentialsFile = File.createTempFile("dummy-credentials", ".json");
    credentialsFile.deleteOnExit();
    Files.write(credentialsFile.toPath(), json.getBytes(StandardCharsets.UTF_8));

    IllegalArgumentException exceptionWithPassword =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new BigQueryCredentialsSupplier(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(credentialsFile.getAbsolutePath()),
                        null,
                        null,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        optionalProxyURI,
                        Optional.empty(),
                        optionalProxyPassword)
                    .getCredentials());

    IllegalArgumentException exceptionWithUserName =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new BigQueryCredentialsSupplier(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(credentialsFile.getAbsolutePath()),
                        null,
                        null,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        optionalProxyURI,
                        optionalProxyUserName,
                        Optional.empty())
                    .getCredentials());

    Arrays.asList(exceptionWithPassword, exceptionWithUserName).stream()
        .forEach(
            exception -> {
              assertThat(exception)
                  .hasMessageThat()
                  .contains(
                      "Both proxyUsername and proxyPassword should be defined or not defined together");
            });
  }

  private String createServiceAccountJson(String projectId) throws Exception {
    GenericJson json = new GenericJson();
    json.setFactory(GsonFactory.getDefaultInstance());
    json.put("type", TYPE);
    json.put("client_id", CLIENT_ID);
    json.put("client_email", CLIENT_EMAIL);
    json.put("private_key", PRIVATE_KEY_PKCS8);
    json.put("private_key_id", PRIVATE_KEY_ID);
    json.put("project_id", projectId);
    json.put("quota_project_id", QUOTA_PROJECT);
    return json.toPrettyString();
  }

  Credentials createImpersonatedCredentials(
      String loggedInUserName,
      Set<String> loggedInUserGroups,
      Map<String, String> userMappings,
      Map<String, String> groupMappings,
      String globalImpersonated) {
    return new BigQueryCredentialsSupplier(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            loggedInUserName,
            loggedInUserGroups,
            Optional.ofNullable(userMappings),
            Optional.ofNullable(groupMappings),
            Optional.ofNullable(globalImpersonated),
            Optional.empty(),
            Optional.empty(),
            Optional.empty())
        .getCredentials();
  }

  @Test
  public void testSingleServiceAccountImpersonation() throws Exception {
    ImpersonatedCredentials credentials =
        (ImpersonatedCredentials)
            createImpersonatedCredentials(null, null, null, null, IMPERSONATED_GLOBAL);
    assertThat(credentials.getSourceCredentials())
        .isEqualTo(GoogleCredentials.getApplicationDefault());
    assertThat(credentials.getAccount()).isEqualTo(IMPERSONATED_GLOBAL);
  }

  @Test
  public void testImpersonationForUsers() throws Exception {
    Map<String, String> userMappings =
        new HashMap<String, String>() {
          {
            put("alice", IMPERSONATED_A);
            put("bob", IMPERSONATED_B);
          }
        };
    Map<String, String> expected =
        new HashMap<String, String>() {
          {
            put("alice", IMPERSONATED_A);
            put("bob", IMPERSONATED_B);
            put("charlie", IMPERSONATED_GLOBAL); // Fallback
          }
        };
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      ImpersonatedCredentials credentials =
          (ImpersonatedCredentials)
              createImpersonatedCredentials(
                  entry.getKey(), null, userMappings, null, IMPERSONATED_GLOBAL);
      assertThat(credentials.getSourceCredentials())
          .isEqualTo(GoogleCredentials.getApplicationDefault());
      assertThat(credentials.getAccount()).isEqualTo(entry.getValue());
    }
  }

  @Test
  public void testImpersonationForGroups() throws Exception {
    Map<String, String> groupMappings =
        new HashMap<String, String>() {
          {
            put("group1", IMPERSONATED_A);
            put("group2", IMPERSONATED_B);
          }
        };
    Map<String, String> expected =
        new HashMap<String, String>() {
          {
            put("group1", IMPERSONATED_A);
            put("group2", IMPERSONATED_B);
            put("group3", IMPERSONATED_GLOBAL); // Fallback
          }
        };
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      ImpersonatedCredentials credentials =
          (ImpersonatedCredentials)
              createImpersonatedCredentials(
                  null,
                  new HashSet<>(Arrays.asList(entry.getKey())),
                  null,
                  groupMappings,
                  IMPERSONATED_GLOBAL);
      assertThat(credentials.getSourceCredentials())
          .isEqualTo(GoogleCredentials.getApplicationDefault());
      assertThat(credentials.getAccount()).isEqualTo(entry.getValue());
    }
  }

  /** Check that the user mappings take precedence over the group mappings. */
  @Test
  public void testImpersonationForUsersAndGroups() throws Exception {
    Map<String, String> userMappings =
        new HashMap<String, String>() {
          {
            put("alice", IMPERSONATED_A);
          }
        };
    Map<String, String> groupMappings =
        new HashMap<String, String>() {
          {
            put("group1", IMPERSONATED_B);
          }
        };
    ImpersonatedCredentials credentials =
        (ImpersonatedCredentials)
            createImpersonatedCredentials(
                "alice",
                new HashSet<>(Arrays.asList("group1")),
                userMappings,
                groupMappings,
                IMPERSONATED_GLOBAL);
    assertThat(credentials.getSourceCredentials())
        .isEqualTo(GoogleCredentials.getApplicationDefault());
    assertThat(credentials.getAccount()).isEqualTo(IMPERSONATED_A);
  }

  @Test
  public void testFallbackToDefault() throws Exception {
    BigQueryCredentialsSupplier supplier =
        new BigQueryCredentialsSupplier(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            null,
            null,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    Credentials credentials = supplier.getCredentials();
    assertThat(credentials).isEqualTo(GoogleCredentials.getApplicationDefault());
  }

  @Test
  public void testExceptionIsThrownOnFile() throws Exception {
    UncheckedIOException e =
        assertThrows(
            UncheckedIOException.class,
            () -> {
              new BigQueryCredentialsSupplier(
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.of("/no/such/file"),
                  null,
                  null,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty());
            });
    assertThat(e.getMessage()).isEqualTo("Failed to create Credentials from file");
    assertThat(e.getCause()).isInstanceOf(FileNotFoundException.class);
  }

  @Test
  public void testExceptionIsThrownOnKey() throws Exception {
    UncheckedIOException e =
        assertThrows(
            UncheckedIOException.class,
            () -> {
              new BigQueryCredentialsSupplier(
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.of("bm8ga2V5IGhlcmU="), // "no key here"
                  Optional.empty(),
                  null,
                  null,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty());
            });
    assertThat(e.getMessage()).isEqualTo("Failed to create Credentials from key");
    assertThat(e.getCause()).isInstanceOf(MalformedJsonException.class);
  }
}
