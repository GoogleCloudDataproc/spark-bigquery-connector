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

import static com.google.common.base.Optional.fromJavaUtil;

import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.common.GenericArrowBigQueryInputPartitionHelper;
import com.google.cloud.spark.bigquery.common.GenericArrowInputPartition;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class ArrowInputPartition implements InputPartition<ColumnarBatch> {
  private GenericArrowInputPartition arrowInputPartitionHelper;

  private final BigQueryClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory tracerFactory;
  private final List<String> streamNames;
  private final ReadRowsHelper.Options options;
  private final ImmutableList<String> selectedFields;
  private final ByteString serializedArrowSchema;
  private final com.google.common.base.Optional<StructType> userProvidedSchema;

  public ArrowInputPartition(
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      List<String> names,
      ReadRowsHelper.Options options,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse,
      Optional<StructType> userProvidedSchema) {
    this.arrowInputPartitionHelper =
        new GenericArrowInputPartition(
            bigQueryReadClientFactory,
            tracerFactory,
            names,
            options,
            selectedFields,
            readSessionResponse,
            userProvidedSchema);
  }

  // this method will be called by spark executors  and it will create partition reader object to
  // read data from Bigquery Streams
  @Override
  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    // instantiate the GenericArrowBigQueryInputPartitionHelper class for each call of partition
    // reader
    GenericArrowBigQueryInputPartitionHelper bqInputPartitionHelper =
        new GenericArrowBigQueryInputPartitionHelper();
    // using generic helper class from dsv 2 parent library to create tracer,read row request object
    //  for each inputPartition reader
    BigQueryStorageReadRowsTracer tracer =
        bqInputPartitionHelper.getBQTracerByStreamNames(
            this.arrowInputPartitionHelper.getTracerFactory(),
            this.arrowInputPartitionHelper.getStreamNames());
    List<ReadRowsRequest.Builder> readRowsRequests =
        bqInputPartitionHelper.getListOfReadRowsRequestsByStreamNames(
            this.arrowInputPartitionHelper.getStreamNames());

    ReadRowsHelper readRowsHelper =
        new ReadRowsHelper(
            this.arrowInputPartitionHelper.getBigQueryReadClientFactory(),
            readRowsRequests,
            this.arrowInputPartitionHelper.getOptions());
    tracer.startStream();
    // iterator to read data from bigquery read rows object
    Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
    return new ArrowColumnBatchPartitionColumnBatchReader(
        readRowsResponses,
        this.arrowInputPartitionHelper.getSerializedArrowSchema(),
        readRowsHelper,
        this.arrowInputPartitionHelper.getSelectedFields(),
        tracer,
        this.arrowInputPartitionHelper.getUserProvidedSchema().toJavaUtil(),
        this.arrowInputPartitionHelper.getOptions().numBackgroundThreads());
  }
}
