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
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.io.UncheckedIOException;

class ParquetIntermediateRecordWriter implements IntermediateRecordWriter {

  private final ParquetWriter parquetWriter;

  ParquetIntermediateRecordWriter(Schema schema, Path path, Configuration conf) {
    try {
      this.parquetWriter =
          AvroParquetWriter.<GenericRecord>builder(path).withSchema(schema).withConf(conf).build();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create AvroParquetWriter", e);
    }
  }

  @Override
  public void write(GenericRecord record) throws IOException {
    parquetWriter.write(record);
  }

  @Override
  public void close() throws IOException {
    parquetWriter.close();
  }
}
