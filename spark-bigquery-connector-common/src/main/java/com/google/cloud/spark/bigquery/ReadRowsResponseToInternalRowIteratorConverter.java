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

import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.javaOptionToGoog;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.googOptionToJava;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

public interface ReadRowsResponseToInternalRowIteratorConverter {

  static ReadRowsResponseToInternalRowIteratorConverter avro(
      final com.google.cloud.bigquery.Schema bqSchema,
      final List<String> columnsInOrder,
      final String rawAvroSchema,
<<<<<<< HEAD
      final Optional<StructType> userProvidedSchema,
      final Optional<BigQueryStorageReadRowsTracer> bigQueryStorageReadRowsTracer) {
    return new Avro(
        bqSchema,
        columnsInOrder,
        rawAvroSchema,
        fromJavaUtil(userProvidedSchema),
        fromJavaUtil(bigQueryStorageReadRowsTracer));
=======
      final Optional<StructType> userProvidedSchema) {
    return new Avro(bqSchema, columnsInOrder, rawAvroSchema, javaOptionToGoog(userProvidedSchema));
>>>>>>> 6fa7b4d (Fix)
  }

  static ReadRowsResponseToInternalRowIteratorConverter arrow(
      final List<String> columnsInOrder,
      final ByteString arrowSchema,
<<<<<<< HEAD
      final Optional<StructType> userProvidedSchema,
      final Optional<BigQueryStorageReadRowsTracer> bigQueryStorageReadRowsTracer) {
    return new Arrow(
        columnsInOrder,
        arrowSchema,
        fromJavaUtil(userProvidedSchema),
        fromJavaUtil(bigQueryStorageReadRowsTracer));
=======
      final Optional<StructType> userProvidedSchema) {
    return new Arrow(columnsInOrder, arrowSchema, javaOptionToGoog(userProvidedSchema));
>>>>>>> 6fa7b4d (Fix)
  }

  Iterator<InternalRow> convert(ReadRowsResponse response);

  int getBatchSizeInBytes(ReadRowsResponse response);

  class Avro implements ReadRowsResponseToInternalRowIteratorConverter, Serializable {

    private final com.google.cloud.bigquery.Schema bqSchema;
    private final List<String> columnsInOrder;
    private final String rawAvroSchema;
    private final com.google.common.base.Optional<StructType> userProvidedSchema;
    private final com.google.common.base.Optional<BigQueryStorageReadRowsTracer>
        bigQueryStorageReadRowsTracer;

    public Avro(
        Schema bqSchema,
        List<String> columnsInOrder,
        String rawAvroSchema,
        com.google.common.base.Optional<StructType> userProvidedSchema,
        com.google.common.base.Optional<BigQueryStorageReadRowsTracer>
            bigQueryStorageReadRowsTracer) {
      this.bqSchema = bqSchema;
      this.columnsInOrder = columnsInOrder;
      this.rawAvroSchema = rawAvroSchema;
      this.userProvidedSchema = userProvidedSchema;
      this.bigQueryStorageReadRowsTracer = bigQueryStorageReadRowsTracer;
    }

    @Override
    public Iterator<InternalRow> convert(ReadRowsResponse response) {
      return new AvroBinaryIterator(
          bqSchema,
          columnsInOrder,
          new org.apache.avro.Schema.Parser().parse(rawAvroSchema),
          response.getAvroRows().getSerializedBinaryRows(),
<<<<<<< HEAD
          userProvidedSchema.toJavaUtil(),
          bigQueryStorageReadRowsTracer.toJavaUtil());
    }

    @Override
    public int getBatchSizeInBytes(ReadRowsResponse response) {
      return response.getAvroRows().getSerializedBinaryRows().size();
=======
          googOptionToJava(userProvidedSchema));
>>>>>>> 6fa7b4d (Fix)
    }
  }

  class Arrow implements ReadRowsResponseToInternalRowIteratorConverter, Serializable {

    private final List<String> columnsInOrder;
    private final ByteString arrowSchema;
    private final com.google.common.base.Optional<StructType> userProvidedSchema;
    private final com.google.common.base.Optional<BigQueryStorageReadRowsTracer>
        bigQueryStorageReadRowsTracer;

    public Arrow(
        List<String> columnsInOrder,
        ByteString arrowSchema,
        com.google.common.base.Optional<StructType> userProvidedSchema,
        com.google.common.base.Optional<BigQueryStorageReadRowsTracer>
            bigQueryStorageReadRowsTracer) {
      this.columnsInOrder = columnsInOrder;
      this.arrowSchema = arrowSchema;
      this.userProvidedSchema = userProvidedSchema;
      this.bigQueryStorageReadRowsTracer = bigQueryStorageReadRowsTracer;
    }

    @Override
    public Iterator<InternalRow> convert(ReadRowsResponse response) {
      return new ArrowBinaryIterator(
          columnsInOrder,
          arrowSchema,
          response.getArrowRecordBatch().getSerializedRecordBatch(),
<<<<<<< HEAD
          userProvidedSchema.toJavaUtil(),
          bigQueryStorageReadRowsTracer.toJavaUtil());
    }

    @Override
    public int getBatchSizeInBytes(ReadRowsResponse response) {
      return response.getArrowRecordBatch().getSerializedRecordBatch().size();
=======
          googOptionToJava(userProvidedSchema));
>>>>>>> 6fa7b4d (Fix)
    }
  }
}
