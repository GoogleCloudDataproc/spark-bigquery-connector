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
