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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;

import java.io.IOException;

class ParquetIntermediateRecordWriter implements IntermediateRecordWriter {

  private final AvroParquetWriter avroParquetWriter;

  ParquetIntermediateRecordWriter(Schema schema, Path path, Configuration conf) {
    this.avroParquetWriter =
        AvroParquetWriter.builder(path).withSchema(schema).withWriteSupport(conf);
  }

  @Override
  public void write(GenericRecord record) throws IOException {
    avroParquetWriter.write(record);
  }

  @Override
  public void close() throws IOException {
    avroParquetWriter.close();
  }
}
