package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDataWriterFactory implements DataWriterFactory<InternalRow> {

  final Logger logger = LoggerFactory.getLogger(BigQueryDataWriterFactory.class);

  private final BigQueryWriteClientFactory writeClientFactory;
  private final String tablePath;
  private final StructType sparkSchema;
  private final ProtoBufProto.ProtoSchema protoSchema;
  private final boolean ignoreInputs;

  public BigQueryDataWriterFactory(
      BigQueryWriteClientFactory writeClientFactory,
      String tablePath,
      StructType sparkSchema,
      ProtoBufProto.ProtoSchema protoSchema,
      boolean ignoreInputs) {
    this.writeClientFactory = writeClientFactory;
    this.tablePath = tablePath;
    this.sparkSchema = sparkSchema;
    this.protoSchema = protoSchema;
    this.ignoreInputs = ignoreInputs;
  }

  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
    if (ignoreInputs) {
      return new NoOpDataWriter();
    }
    return new BigQueryDataWriter(
        partitionId,
        taskId,
        epochId,
        writeClientFactory,
        tablePath,
        sparkSchema,
        protoSchema,
        ignoreInputs);
  }
}
