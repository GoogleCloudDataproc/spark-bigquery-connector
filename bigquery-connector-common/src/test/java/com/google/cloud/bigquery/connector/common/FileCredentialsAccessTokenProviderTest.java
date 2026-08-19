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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.junit.Test;

public class FileCredentialsAccessTokenProviderTest {

  @Test
  public void testSerialization() {
    String path = "/path/to/credentials.json";
    FileCredentialsAccessTokenProvider provider = new FileCredentialsAccessTokenProvider(path);
    assertThat(provider.getCredentialsPath()).isEqualTo(path);

    FileCredentialsAccessTokenProvider deserialized = BigQueryUtil.verifySerialization(provider);
    assertThat(deserialized.getCredentialsPath()).isEqualTo(path);
  }

  @Test
  public void testNullCredentialsPathThrows() {
    FileCredentialsAccessTokenProvider provider = new FileCredentialsAccessTokenProvider();
    assertThrows(IllegalArgumentException.class, provider::getAccessToken);
  }

  @Test
  public void testEmptyCredentialsPathThrows() {
    FileCredentialsAccessTokenProvider provider = new FileCredentialsAccessTokenProvider("  ");
    assertThrows(IllegalArgumentException.class, provider::getAccessToken);
  }

  @Test
  public void testNonExistentFileThrows() {
    FileCredentialsAccessTokenProvider provider =
        new FileCredentialsAccessTokenProvider("/non/existent/path/credentials.json");
    assertThrows(FileNotFoundException.class, provider::getAccessToken);
  }

  @Test
  public void testInvalidJsonCredentialsFileThrows() throws IOException {
    File tempFile = File.createTempFile("invalid-credentials", ".json");
    tempFile.deleteOnExit();
    Files.write(tempFile.toPath(), "invalid json".getBytes(StandardCharsets.UTF_8));

    FileCredentialsAccessTokenProvider provider =
        new FileCredentialsAccessTokenProvider(tempFile.getAbsolutePath());
    assertThrows(IOException.class, provider::getAccessToken);
  }
}
