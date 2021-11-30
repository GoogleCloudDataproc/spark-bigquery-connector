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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

public class GenericAvroIntermediateRecordWriter implements Serializable, IntermediateRecordWriter {
  private final OutputStream outputStream;
  private final DatumWriter<GenericRecord> writer;
  private final DataFileWriter<GenericRecord> dataFileWriter;

  public GenericAvroIntermediateRecordWriter(Schema schema, OutputStream outputStream)
      throws IOException {
    this.outputStream = outputStream;
    this.writer = new GenericDatumWriter<>(schema);
    this.dataFileWriter = new DataFileWriter<>(writer);
    this.dataFileWriter.create(schema, outputStream);
  }

  public DataFileWriter<GenericRecord> getDataFileWriter() {
    return this.dataFileWriter;
  }

  @Override
  public void write(GenericRecord avroRecord) throws IOException {
    this.dataFileWriter.append(avroRecord);
  }

  @Override
  public void close() throws IOException {
    try {
      this.dataFileWriter.flush();
    } finally {
      this.dataFileWriter.close();
    }
  }
}
