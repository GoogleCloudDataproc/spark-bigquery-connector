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
import java.io.Serializable;

public class Link implements Serializable {

  private static final long serialVersionUID = 914804886152047390L;
  private String uri;

  public Link() {}

  public Link(String uri) {
    this.uri = uri;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Link)) {
      return false;
    }
    Link link = (Link) o;
    return Objects.equal(uri, link.uri);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(uri);
  }

  @Override
  public String toString() {
    return "Link{" + "uri='" + uri + '\'' + '}';
  }
}
