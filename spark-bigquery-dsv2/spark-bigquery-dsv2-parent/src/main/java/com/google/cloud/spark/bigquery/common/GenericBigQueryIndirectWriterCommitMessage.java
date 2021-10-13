package com.google.cloud.spark.bigquery.common;

public class GenericBigQueryIndirectWriterCommitMessage {
  private final String uri;

  public GenericBigQueryIndirectWriterCommitMessage(String uri) {
    this.uri = uri;
  }

  public String getUri() {
    return this.uri;
  }
}
