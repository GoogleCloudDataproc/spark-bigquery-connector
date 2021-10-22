package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.common.GenericBigQueryIndirectDataWriterFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

class BigQueryIndirectDataWriterFactory extends GenericBigQueryIndirectDataWriterFactory
    implements DataWriterFactory {
  SerializableConfiguration conf;
  StructType sparkSchema;

  public BigQueryIndirectDataWriterFactory(
      SerializableConfiguration conf,
      String gcsDirPath,
      StructType sparkSchema,
      String avroSchemaJson) {
    super(gcsDirPath, avroSchemaJson);
    this.conf = conf;
    this.sparkSchema = sparkSchema;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    try {
      enableDataWriter(partitionId, taskId, -1L);
      FileSystem fs = getPath().getFileSystem(conf.get());
      IntermediateRecordWriter intermediateRecordWriter =
          new AvroIntermediateRecordWriter(getAvroSchema(), fs.create(getPath()));
      return new BigQueryIndirectDataWriter(
          partitionId, getPath(), fs, sparkSchema, getAvroSchema(), intermediateRecordWriter);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
