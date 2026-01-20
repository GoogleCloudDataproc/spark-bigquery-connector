package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.write.context.DataWriterContext;
import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class BigQueryStreamingDataWriter implements DataWriter<InternalRow> {
  private final DataWriterContext<InternalRow> ctx;

  public BigQueryStreamingDataWriter(DataWriterContext<InternalRow> ctx) {
    this.ctx = ctx;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    ctx.write(record);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    return new Spark31BigQueryWriterCommitMessage(ctx.commit());
  }

  @Override
  public void abort() throws IOException {
    ctx.abort();
  }

  @Override
  public void close() throws IOException {
    ctx.close();
  }
}
