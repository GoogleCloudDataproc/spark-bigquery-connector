package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.AvroSchemaConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.UncheckedIOException;

public class AvroIntermediateRecordWriter implements IntermediateRecordWriter {

  StructType sparkSchema;
  Schema avroSchema;
  DatumWriter<GenericRecord> writer;
  String path;
  FSDataOutputStream outputStream;
  BinaryEncoder encoder;

  AvroIntermediateRecordWriter(
      StructType sparkSchema, Schema schema, String path, Configuration conf) {
    try {
      this.sparkSchema = sparkSchema;
      this.writer = new GenericDatumWriter<>(schema);
      this.path = path;
      Path hdfsPath = new Path(path);
      FileSystem fs = hdfsPath.getFileSystem(conf);
      this.outputStream = fs.create(hdfsPath);
      this.encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void write(InternalRow record) throws IOException {
    GenericRecord avroRecord =
        AvroSchemaConverter.sparkRowToAvroGenericData(record, sparkSchema, avroSchema);
    writer.write(avroRecord, encoder);
  }

  @Override
  public void close() throws IOException {
    try {
      encoder.flush();
    } finally {
      outputStream.close();
    }
  }

  @Override
  public String getPath() {
    return path;
  }
}
