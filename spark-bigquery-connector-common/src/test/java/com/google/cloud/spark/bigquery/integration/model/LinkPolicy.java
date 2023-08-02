/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.integration.model;

import com.google.common.base.Objects;

public class LinkPolicy {

  private String uri;
  private String uriPolicy;

  public LinkPolicy() {}

  public LinkPolicy(String uri, String uriPolicy) {
    this.uri = uri;
    this.uriPolicy = uriPolicy;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  public String getUriPolicy() {
    return uriPolicy;
  }

  public void setUriPolicy(String uriPolicy) {
    this.uriPolicy = uriPolicy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LinkPolicy)) {
      return false;
    }
    LinkPolicy linkPolicy = (LinkPolicy) o;
    return Objects.equal(uri, linkPolicy.uri) && Objects.equal(uriPolicy, linkPolicy.uriPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(uri, uriPolicy);
  }

  @Override
  public String toString() {
    return "LinkPolicy{" + "uri='" + uri + '\'' + ", uriPolicy=" + uriPolicy + '}';
  }
}
