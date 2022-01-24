/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.HeaderProvider;
import java.util.Map;
import org.junit.Test;

public class HttpUtilTest {

  @Test
  public void testCreateHeaderProviderWithParentProject() {
    BigQueryConfig config = mock(BigQueryConfig.class);
    when(config.useParentProjectForMetadataOperations()).thenReturn(true);
    when(config.getParentProjectId()).thenReturn("User-Project");

    HeaderProvider headerProvider = HttpUtil.createHeaderProvider(config, "UA");
    Map<String, String> headers = headerProvider.getHeaders();
    assertThat(headers).hasSize(2);
    assertThat(headers.get("X-Goog-User-Project")).isEqualTo("User-Project");
    assertThat(headers.get("user-agent")).isEqualTo("UA");
  }

  @Test
  public void testCreateHeaderProviderNoParentProject() {
    BigQueryConfig config = mock(BigQueryConfig.class);
    when(config.useParentProjectForMetadataOperations()).thenReturn(false);

    HeaderProvider headerProvider = HttpUtil.createHeaderProvider(config, "UA");
    Map<String, String> headers = headerProvider.getHeaders();
    assertThat(headers).hasSize(1);
    assertThat(headers.get("user-agent")).isEqualTo("UA");
  }
}
