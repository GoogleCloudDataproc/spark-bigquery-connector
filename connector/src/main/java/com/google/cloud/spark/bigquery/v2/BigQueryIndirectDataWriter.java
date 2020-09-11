package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.io.IOException;

public class BigQueryIndirectDataWriter implements DataWriter<InternalRow> {
    @Override
    public void write(InternalRow record) throws IOException {

    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        return new BigQueryIndirectWriterCommitMessage();
    }

    @Override
    public void abort() throws IOException {

    }
}
