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
package com.google.cloud.spark.bigquery.common;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroIntermediateRecordWriter implements IntermediateRecordWriter, Serializable {
  private GenericAvroIntermediateRecordWriter avroIntermediateRecordWriterHelper;

  AvroIntermediateRecordWriter(Schema schema, OutputStream outputStream) throws IOException {
    this.avroIntermediateRecordWriterHelper =
        new GenericAvroIntermediateRecordWriter(schema, outputStream);
  }

  @Override
  public void write(GenericRecord record) throws IOException {
    this.avroIntermediateRecordWriterHelper.write(record);
  }

  @Override
  public void close() throws IOException {
    this.avroIntermediateRecordWriterHelper.close();
  }
}
