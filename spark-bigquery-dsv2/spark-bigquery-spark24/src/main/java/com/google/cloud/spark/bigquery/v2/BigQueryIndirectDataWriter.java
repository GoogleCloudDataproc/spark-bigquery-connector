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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.AvroSchemaConverter;
import com.google.cloud.spark.bigquery.common.GenericBigQueryIndirectDataWriter;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigQueryIndirectDataWriter extends GenericBigQueryIndirectDataWriter
    implements DataWriter<InternalRow> {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryIndirectDataWriter.class);
  FSDataOutputStream outputStream;
  StructType sparkSchema;
  IntermediateRecordWriter intermediateRecordWriter;

  protected BigQueryIndirectDataWriter(
      int partitionId,
      Path path,
      FileSystem fs,
      StructType sparkSchema,
      Schema avroSchema,
      IntermediateRecordWriter intermediateRecordWriter) {
    super(partitionId, path, fs, avroSchema);
    this.sparkSchema = sparkSchema;
    this.intermediateRecordWriter = intermediateRecordWriter;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    GenericRecord avroRecord =
        AvroSchemaConverter.sparkRowToAvroGenericData(record, sparkSchema, getAvroSchema());
    intermediateRecordWriter.write(avroRecord);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    intermediateRecordWriter.close();
    return new BigQueryIndirectWriterCommitMessage(getPath().toString());
  }

  @Override
  public void abort() throws IOException {
    logger.warn(
        "Writing of partition {} has been aborted, attempting to delete {}",
        getPartitionId(),
        getPath());
    getFs().delete(getPath(), false);
  }
}
