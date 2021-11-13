package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.common.GenericBigQueryIndirectWriterCommitMessage;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

class BigQueryIndirectWriterCommitMessage implements WriterCommitMessage {

  private final GenericBigQueryIndirectWriterCommitMessage commitMessage;

  public BigQueryIndirectWriterCommitMessage(String uri) {
    this.commitMessage = new GenericBigQueryIndirectWriterCommitMessage(uri);
  }

  public String getUri() {
    return this.commitMessage.getUri();
  }
}
