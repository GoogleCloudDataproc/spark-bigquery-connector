package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BigQueryIndirectDataWriter implements DataWriter<InternalRow> {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryIndirectDataWriter.class);

  private IntermediateRecordWriter intermediateRecordWriter;

  @Override
  public void write(InternalRow record) throws IOException {
    intermediateRecordWriter.write(record);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    intermediateRecordWriter.close();
    return new BigQueryIndirectWriterCommitMessage(intermediateRecordWriter.getPath());
  }

  @Override
  public void abort() throws IOException {}
}
