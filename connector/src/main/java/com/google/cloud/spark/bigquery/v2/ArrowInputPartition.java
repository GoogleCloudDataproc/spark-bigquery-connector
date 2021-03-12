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


import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;

public class ArrowInputPartition implements InputPartition<ColumnarBatch> {

  private final BigQueryReadClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory tracerFactory;
  private final String streamName;
  private final int maxReadRowsRetries;
  private final ImmutableList<String> selectedFields;
  private final ByteString serializedArrowSchema;

  public ArrowInputPartition(
      BigQueryReadClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      String name,
      int maxReadRowsRetries,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse) {
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.streamName = name;
    this.maxReadRowsRetries = maxReadRowsRetries;
    this.selectedFields = selectedFields;
    this.serializedArrowSchema =
        readSessionResponse.getReadSession().getArrowSchema().getSerializedSchema();
    this.tracerFactory = tracerFactory;
  }

  @Override
  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    BigQueryStorageReadRowsTracer tracer = tracerFactory.newReadRowsTracer(streamName);
    ReadRowsRequest.Builder readRowsRequest =
        ReadRowsRequest.newBuilder().setReadStream(streamName);
    ReadRowsHelper readRowsHelper =
        new ReadRowsHelper(bigQueryReadClientFactory, readRowsRequest, maxReadRowsRetries);
    tracer.startStream();
    Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
    return new ArrowColumnBatchPartitionColumnBatchReader(
        readRowsResponses, serializedArrowSchema, readRowsHelper, selectedFields, tracer);
  }
}
