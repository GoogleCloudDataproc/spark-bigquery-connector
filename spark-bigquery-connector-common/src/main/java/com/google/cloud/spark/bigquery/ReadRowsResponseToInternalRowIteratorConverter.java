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

import static com.google.common.base.Optional.fromJavaUtil;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

public interface ReadRowsResponseToInternalRowIteratorConverter {

  static ReadRowsResponseToInternalRowIteratorConverter avro(
      final Schema bqSchema,
      final List<String> columnsInOrder,
      final String rawAvroSchema,
      final Optional<StructType> userProvidedSchema,
      final Optional<BigQueryStorageReadRowsTracer> bigQueryStorageReadRowsTracer,
      final SchemaConvertersConfiguration schemaConvertersConfiguration,
      final ResponseCompressionCodec responseCompressionCodec) {
    return new Avro(
        bqSchema,
        columnsInOrder,
        rawAvroSchema,
        fromJavaUtil(userProvidedSchema),
        fromJavaUtil(bigQueryStorageReadRowsTracer),
        schemaConvertersConfiguration,
        responseCompressionCodec);
  }

  static ReadRowsResponseToInternalRowIteratorConverter arrow(
      final List<String> columnsInOrder,
      final ByteString arrowSchema,
      final Optional<StructType> userProvidedSchema,
      final Optional<BigQueryStorageReadRowsTracer> bigQueryStorageReadRowsTracer,
      final ResponseCompressionCodec responseCompressionCodec) {
    return new Arrow(
        columnsInOrder,
        arrowSchema,
        fromJavaUtil(userProvidedSchema),
        fromJavaUtil(bigQueryStorageReadRowsTracer),
        responseCompressionCodec);
  }

  Iterator<InternalRow> convert(ReadRowsResponse response);

  int getBatchSizeInBytes(ReadRowsResponse response);

  class Avro implements ReadRowsResponseToInternalRowIteratorConverter, Serializable {

    private static final long serialVersionUID = 2994401325376236028L;
    private final com.google.cloud.bigquery.Schema bqSchema;
    private final List<String> columnsInOrder;
    private final String rawAvroSchema;
    private final com.google.common.base.Optional<StructType> userProvidedSchema;
    private final com.google.common.base.Optional<BigQueryStorageReadRowsTracer>
        bigQueryStorageReadRowsTracer;
    private final SchemaConvertersConfiguration schemaConvertersConfiguration;
    private final ResponseCompressionCodec responseCompressionCodec;

    public Avro(
        Schema bqSchema,
        List<String> columnsInOrder,
        String rawAvroSchema,
        com.google.common.base.Optional<StructType> userProvidedSchema,
        com.google.common.base.Optional<BigQueryStorageReadRowsTracer>
            bigQueryStorageReadRowsTracer,
        SchemaConvertersConfiguration schemaConvertersConfiguration,
        ResponseCompressionCodec responseCompressionCodec) {
      this.bqSchema = bqSchema;
      this.columnsInOrder = columnsInOrder;
      this.rawAvroSchema = rawAvroSchema;
      this.userProvidedSchema = userProvidedSchema;
      this.bigQueryStorageReadRowsTracer = bigQueryStorageReadRowsTracer;
      this.schemaConvertersConfiguration = schemaConvertersConfiguration;
      this.responseCompressionCodec = responseCompressionCodec;
    }

    @Override
    public Iterator<InternalRow> convert(ReadRowsResponse response) {
      return new AvroBinaryIterator(
          bqSchema,
          columnsInOrder,
          new org.apache.avro.Schema.Parser().parse(rawAvroSchema),
          response,
          userProvidedSchema.toJavaUtil(),
          bigQueryStorageReadRowsTracer.toJavaUtil(),
          schemaConvertersConfiguration,
          responseCompressionCodec);
    }

    @Override
    public int getBatchSizeInBytes(ReadRowsResponse response) {
      if (response.getUncompressedByteSize() > 0) {
        return (int) response.getUncompressedByteSize();
      }
      return response.getAvroRows().getSerializedBinaryRows().size();
    }
  }

  class Arrow implements ReadRowsResponseToInternalRowIteratorConverter, Serializable {

    private static final long serialVersionUID = -524013378047083342L;
    private final List<String> columnsInOrder;
    private final ByteString arrowSchema;
    private final com.google.common.base.Optional<StructType> userProvidedSchema;
    private final com.google.common.base.Optional<BigQueryStorageReadRowsTracer>
        bigQueryStorageReadRowsTracer;
    private final ResponseCompressionCodec responseCompressionCodec;

    public Arrow(
        List<String> columnsInOrder,
        ByteString arrowSchema,
        com.google.common.base.Optional<StructType> userProvidedSchema,
        com.google.common.base.Optional<BigQueryStorageReadRowsTracer>
            bigQueryStorageReadRowsTracer,
        ResponseCompressionCodec responseCompressionCodec) {
      this.columnsInOrder = columnsInOrder;
      this.arrowSchema = arrowSchema;
      this.userProvidedSchema = userProvidedSchema;
      this.bigQueryStorageReadRowsTracer = bigQueryStorageReadRowsTracer;
      this.responseCompressionCodec = responseCompressionCodec;
    }

    @Override
    public Iterator<InternalRow> convert(ReadRowsResponse response) {

      return new ArrowBinaryIterator(
          columnsInOrder,
          arrowSchema,
          response,
          userProvidedSchema.toJavaUtil(),
          bigQueryStorageReadRowsTracer.toJavaUtil(),
          responseCompressionCodec);
    }

    @Override
    public int getBatchSizeInBytes(ReadRowsResponse response) {
      if (response.getUncompressedByteSize() > 0) {
        return (int) response.getUncompressedByteSize();
      }
      return response.getArrowRecordBatch().getSerializedRecordBatch().size();
    }
  }
}
