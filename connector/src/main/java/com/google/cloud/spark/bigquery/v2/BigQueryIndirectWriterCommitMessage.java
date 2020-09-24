package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

public class BigQueryIndirectWriterCommitMessage implements WriterCommitMessage {

  private final String uri;

  public BigQueryIndirectWriterCommitMessage(String uri) {
    this.uri = uri;
  }

  public String getUri() {
    return uri;
  }
}
