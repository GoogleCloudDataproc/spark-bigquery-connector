package com.google.cloud.spark.bigquery.v2;
import com.google.cloud.spark.bigquery.common.GenericBigQueryIndirectWriterCommitMessage;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

class BigQueryIndirectWriterCommitMessage extends GenericBigQueryIndirectWriterCommitMessage
        implements WriterCommitMessage {

    public BigQueryIndirectWriterCommitMessage(String uri) {
        super(uri);
    }
}