package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.ProtobufUtils;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public class ParquetIntermediateRecordWriter implements IntermediateRecordWriter {

  // private final RecordWriter<Void, InternalRow>
  private final StructType schema;
  private final Descriptors.Descriptor schemaDescriptor;

  public ParquetIntermediateRecordWriter(StructType schema) {
    this.schema = schema;
    this.schemaDescriptor = ProtobufUtils.toDescriptor(schema);
  }

  @Override
  public void write(InternalRow record) throws IOException {
    Message message = ProtobufUtils.buildSingleRowMessage(schema, schemaDescriptor, record);
  }

  @Override
  public void close() throws IOException {}

  @Override
  public String getPath() {
    return null;
  }
}
