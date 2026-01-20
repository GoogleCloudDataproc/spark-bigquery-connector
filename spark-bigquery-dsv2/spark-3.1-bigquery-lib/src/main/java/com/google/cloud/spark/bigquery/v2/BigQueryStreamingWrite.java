package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.write.context.DataSourceWriterContext;
import com.google.cloud.spark.bigquery.write.context.WriterCommitMessageContext;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

public class BigQueryStreamingWrite implements StreamingWrite {
  private final DataSourceWriterContext ctx;

  public BigQueryStreamingWrite(DataSourceWriterContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
    return new BigQueryStreamingWriterFactory(ctx.createWriterContextFactory());
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    this.ctx.onDataStreamingWriterCommit(epochId, toWriterCommitMessageContextArray(messages));
  }

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {
    this.ctx.onDataStreamingWriterAbort(epochId, toWriterCommitMessageContextArray(messages));
  }

  private WriterCommitMessageContext toWriterCommitMessageContext(WriterCommitMessage message) {
    return ((Spark31BigQueryWriterCommitMessage) message).getContext();
  }

  private WriterCommitMessageContext[] toWriterCommitMessageContextArray(
      WriterCommitMessage[] messages) {
    return Stream.of(messages)
        .map(this::toWriterCommitMessageContext)
        .toArray(WriterCommitMessageContext[]::new);
  }
}
