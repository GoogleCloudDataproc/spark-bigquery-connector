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
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.DecompressReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroBinaryIterator implements Iterator<InternalRow> {

  private static final Logger log = LoggerFactory.getLogger(AvroBinaryIterator.class);
  private final Optional<BigQueryStorageReadRowsTracer> bigQueryStorageReadRowsTracer;
  private long numberOfRowsParsed = 0;
  GenericDatumReader reader;
  List<String> columnsInOrder;
  BinaryDecoder in;
  Schema bqSchema;
  Optional<StructType> userProvidedSchema;

  private final SchemaConverters schemaConverters;

  /**
   * An iterator for scanning over rows serialized in Avro format
   *
   * @param bqSchema Schema of underlying BigQuery source
   * @param columnsInOrder Sequence of columns in the schema
   * @param schema Schema in avro format
   * @param rowsInBytes Rows serialized in binary format for Avro
   */
  public AvroBinaryIterator(
      Schema bqSchema,
      List<String> columnsInOrder,
      org.apache.avro.Schema schema,
      ReadRowsResponse response,
      Optional<StructType> userProvidedSchema,
      Optional<BigQueryStorageReadRowsTracer> bigQueryStorageReadRowsTracer,
      SchemaConvertersConfiguration schemaConvertersConfiguration,
      ResponseCompressionCodec responseCompressionCodec) {
    reader = new GenericDatumReader<GenericRecord>(schema);
    this.bqSchema = bqSchema;
    this.columnsInOrder = columnsInOrder;
    try {
      in =
          new DecoderFactory()
              .binaryDecoder(
                  DecompressReadRowsResponse.decompressAvroRecordBatch(
                      response, responseCompressionCodec),
                  null);
    } catch (IOException e) {
      throw new BigQueryConnectorException(
          "Failed to decode Avro binary for a ReadRowsResponse", e);
    }
    this.userProvidedSchema = userProvidedSchema;
    this.bigQueryStorageReadRowsTracer = bigQueryStorageReadRowsTracer;
    this.schemaConverters = SchemaConverters.from(schemaConvertersConfiguration);
  }

  @Override
  public boolean hasNext() {
    try {
      // Avro iterator is used in both V1 and V2
      if (bigQueryStorageReadRowsTracer.isPresent() && in.isEnd()) {
        // Avro parsing is done row by row as opposed to Arrow where whole batch is processed once.
        // Due to this, row parse time could potentially include some application time.
        // One way to address this is to update tracer interface to support recording per row
        // parsing time, along with batch mode.
        bigQueryStorageReadRowsTracer.get().rowsParseFinished(numberOfRowsParsed);
      }
      return !in.isEnd();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public InternalRow next() {
    try {
      numberOfRowsParsed++;
      return schemaConverters.convertToInternalRow(
          bqSchema, columnsInOrder, (GenericRecord) reader.read(null, in), userProvidedSchema);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
