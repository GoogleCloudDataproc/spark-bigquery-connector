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

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import java.util.HashMap;
import java.util.Map;

public class HttpUtil {

  private HttpUtil() {}

  public static HeaderProvider createHeaderProvider(BigQueryConfig config, String userAgent) {
    Map<String, String> headers = new HashMap<>();
    if (config.useParentProjectForMetadataOperations()) {
      headers.put("X-Goog-User-Project", config.getParentProjectId());
    }
    headers.put("user-agent", userAgent);
    return FixedHeaderProvider.create(headers);
  }
}
