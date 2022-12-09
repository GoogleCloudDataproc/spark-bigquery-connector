/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.write.context;

import com.google.cloud.spark.bigquery.AvroSchemaConverter;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigQueryIndirectDataWriterContext implements DataWriterContext<InternalRow> {

  private static final Logger logger =
      LoggerFactory.getLogger(BigQueryIndirectDataWriterContext.class);
  Path path;
  FileSystem fs;
  FSDataOutputStream outputStream;
  StructType sparkSchema;
  Schema avroSchema;
  IntermediateRecordWriter intermediateRecordWriter;
  private int partitionId;

  protected BigQueryIndirectDataWriterContext(
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

  @Override
  public void write(InternalRow record) throws IOException {
    GenericRecord avroRecord =
        AvroSchemaConverter.sparkRowToAvroGenericData(record, sparkSchema, avroSchema);
    intermediateRecordWriter.write(avroRecord);
  }

  @Override
  public WriterCommitMessageContext commit() throws IOException {
    intermediateRecordWriter.flush();
    return new BigQueryIndirectWriterCommitMessageContext(path.toString());
  }

  @Override
  public void abort() throws IOException {
    logger.warn(
        "Writing of partition {} has been aborted, attempting to delete {}", partitionId, path);
    fs.delete(path, false);
  }

  @Override
  public void close() throws IOException {
    intermediateRecordWriter.close();
  }
}
