package com.google.cloud.spark.bigquery.common;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GenericBigQueryIndirectDataWriter {
  Path path;
  FileSystem fs;
  FSDataOutputStream outputStream;
  Schema avroSchema;
  private int partitionId;

  public GenericBigQueryIndirectDataWriter(
      int partitionId, Path path, FileSystem fs, Schema avroSchema) {
    this.partitionId = partitionId;
    this.path = path;
    this.fs = fs;
    this.avroSchema = avroSchema;
  }

  public Path getPath() {
    return path;
  }

  public FileSystem getFs() {
    return fs;
  }

  public FSDataOutputStream getOutputStream() {
    return outputStream;
  }

  public Schema getAvroSchema() {
    return avroSchema;
  }

  public int getPartitionId() {
    return partitionId;
  }
}
