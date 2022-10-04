/*
 * Copyright 2022 Google LLC
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

import java.net.URI;
import java.util.Optional;

/**
 * Config interface to provide proxy parameters. The concrete class implementing this interface
 * should implement methods to provide ProxyUri, ProxyUserName and ProxyPassword.
 */
public interface BigQueryProxyConfig {

  /**
   * The Uniform Resource Identifier ({@link URI URI}) of the proxy sever. This is an Optional field
   * so can be empty.
   *
   * @return java.util.Optional<URI>
   */
  Optional<URI> getProxyUri();

  /**
   * The userName used to connect to the proxy. This is an Optional field so can be empty.
   *
   * @return java.util.Optional<String>
   */
  Optional<String> getProxyUsername();

  /**
   * The password used to connect to the proxy. This is an Optional field so can be empty.
   *
   * @return java.util.Optional<String>
   */
  Optional<String> getProxyPassword();
}
