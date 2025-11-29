package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.write.context.DataWriterContextFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;

public class BigQueryStreamingWriterFactory implements StreamingDataWriterFactory {

  private final DataWriterContextFactory<InternalRow> writerContextFactory;

  public BigQueryStreamingWriterFactory(
      DataWriterContextFactory<InternalRow> writerContextFactory) {
    this.writerContextFactory = writerContextFactory;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
    return new BigQueryStreamingDataWriter(
        writerContextFactory.createDataWriterContext(partitionId, taskId, 0));
  }
}
