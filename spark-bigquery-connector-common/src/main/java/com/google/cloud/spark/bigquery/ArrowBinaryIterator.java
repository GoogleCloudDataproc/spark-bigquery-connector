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

import com.google.cloud.bigquery.connector.common.ArrowReaderIterator;
import com.google.cloud.bigquery.connector.common.ArrowUtil;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.DecompressReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class ArrowBinaryIterator implements Iterator<InternalRow> {

  private static long maxAllocation = Long.MAX_VALUE;
  private final Optional<BigQueryStorageReadRowsTracer> bigQueryStorageReadRowsTracer;
  ArrowReaderIterator arrowReaderIterator;
  Iterator<InternalRow> currentIterator;
  List<String> columnsInOrder;
  Map<String, StructField> userProvidedFieldMap;

  public ArrowBinaryIterator(
      List<String> columnsInOrder,
      ByteString schema,
      ReadRowsResponse readRowsResponse,
      Optional<StructType> userProvidedSchema,
      Optional<BigQueryStorageReadRowsTracer> bigQueryStorageReadRowsTracer,
      ResponseCompressionCodec responseCompressionCodec) {
    BufferAllocator allocator =
        ArrowUtil.newRootAllocator(maxAllocation)
            .newChildAllocator("ArrowBinaryIterator", 0, maxAllocation);

    try {
      SequenceInputStream bytesWithSchemaStream =
          new SequenceInputStream(
              new ByteArrayInputStream(schema.toByteArray()),
              DecompressReadRowsResponse.decompressArrowRecordBatch(
                  readRowsResponse, responseCompressionCodec));
      ArrowStreamReader arrowStreamReader =
          new ArrowStreamReader(
              bytesWithSchemaStream, allocator, CommonsCompressionFactory.INSTANCE);
      arrowReaderIterator = new ArrowReaderIterator(arrowStreamReader);
    } catch (IOException e) {
      throw new BigQueryConnectorException(
          "Failed to decode Arrow binary for a ReadRowsResponse", e);
    }
    currentIterator = ImmutableList.<InternalRow>of().iterator();
    this.columnsInOrder = columnsInOrder;

    List<StructField> userProvidedFieldList =
        Arrays.stream(userProvidedSchema.orElse(new StructType()).fields())
            .collect(Collectors.toList());

    this.userProvidedFieldMap =
        userProvidedFieldList.stream()
            .collect(Collectors.toMap(StructField::name, Function.identity()));

    this.bigQueryStorageReadRowsTracer = bigQueryStorageReadRowsTracer;
  }

  @Override
  public boolean hasNext() {
    while (!currentIterator.hasNext()) {
      if (!arrowReaderIterator.hasNext()) {
        return false;
      }
      currentIterator = toArrowRows(arrowReaderIterator.next(), columnsInOrder);
    }

    return currentIterator.hasNext();
  }

  @Override
  public InternalRow next() {
    return currentIterator.next();
  }

  private Iterator<InternalRow> toArrowRows(VectorSchemaRoot root, List<String> namesInOrder) {
    ColumnVector[] columns =
        namesInOrder.stream()
            .map(name -> root.getVector(name))
            .map(
                vector ->
                    ArrowSchemaConverter.newArrowSchemaConverter(
                        vector, userProvidedFieldMap.get(vector.getName())))
            .collect(Collectors.toList())
            .toArray(new ColumnVector[0]);

    ColumnarBatch batch = new ColumnarBatch(columns);
    batch.setNumRows(root.getRowCount());
    bigQueryStorageReadRowsTracer.ifPresent(tracer -> tracer.rowsParseFinished(root.getRowCount()));
    return batch.rowIterator();
  }
}
