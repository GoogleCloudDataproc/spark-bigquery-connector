/*
 * Copyright 2021 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.common;

import com.google.cloud.spark.bigquery.AvroSchemaConverter;
import java.io.IOException;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

public class GenericBigQueryIndirectDataWriter implements Serializable {
  Path path;
  FileSystem fs;
  FSDataOutputStream outputStream;
  Schema avroSchema;
  private int partitionId;
  StructType sparkSchema;
  private boolean fileClosed = false;
  IntermediateRecordWriter intermediateRecordWriter;

  public GenericBigQueryIndirectDataWriter(
      int partitionId,
      Path path,
      FileSystem fs,
      StructType sparkSchema,
      Schema avroSchema,
      IntermediateRecordWriter intermediateRecordWriter) {
    this.partitionId = partitionId;
    this.path = path;
    this.fs = fs;
    this.sparkSchema = sparkSchema;
    this.avroSchema = avroSchema;
    this.intermediateRecordWriter = intermediateRecordWriter;
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

  public StructType getSparkSchema() {
    return sparkSchema;
  }

  public IntermediateRecordWriter getIntermediateRecordWriter() {
    return intermediateRecordWriter;
  }

  public void commitRecord() throws IOException {
    intermediateRecordWriter.close();
  }

  public void writeRecord(InternalRow record) throws IOException {
    GenericRecord avroRecord =
        AvroSchemaConverter.sparkRowToAvroGenericData(record, getSparkSchema(), getAvroSchema());
    intermediateRecordWriter.write(avroRecord);
  }

  public void writeAbort() throws IOException {
    this.fs.delete(path, false);
  }

  public void close() throws IOException {
    if (!this.fileClosed) {
      this.fs.close();
      this.fileClosed = true;
    }
  }
}
