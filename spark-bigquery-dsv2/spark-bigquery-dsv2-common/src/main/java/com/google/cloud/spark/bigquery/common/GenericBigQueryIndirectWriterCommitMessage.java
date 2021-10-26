package com.google.cloud.spark.bigquery.common;

import java.io.Serializable;

public class GenericBigQueryIndirectWriterCommitMessage implements Serializable {
  private final String uri;

  public GenericBigQueryIndirectWriterCommitMessage(String uri) {
    this.uri = uri;
  }

  public String getUri() {
    return this.uri;
  }
}
