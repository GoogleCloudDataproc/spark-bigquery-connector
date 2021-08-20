package com.google.cloud.spark.bigquery.integration.model;

import com.google.common.base.Objects;
import java.io.Serializable;

public class Link implements Serializable {

  private String uri;

  public Link() {
  }

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
    return "Link{" +
        "uri='" + uri + '\'' +
        '}';
  }
}
