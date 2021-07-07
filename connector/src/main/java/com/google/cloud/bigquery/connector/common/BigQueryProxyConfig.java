package com.google.cloud.bigquery.connector.common;

import java.net.URI;
import java.util.Optional;

public interface BigQueryProxyConfig {
  Optional<URI> getProxyUri();

  Optional<String> getProxyUsername();

  Optional<String> getProxyPassword();
}
