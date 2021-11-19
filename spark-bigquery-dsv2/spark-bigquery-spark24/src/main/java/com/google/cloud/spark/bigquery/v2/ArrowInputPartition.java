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
import com.google.cloud.spark.bigquery.common.GenericArrowInputPartition;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class ArrowInputPartition implements InputPartition<ColumnarBatch> {
  private GenericArrowInputPartition arrowInputPartitionHelper;

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
    this.arrowInputPartitionHelper.createPartitionReader();
    return new ArrowColumnBatchPartitionColumnBatchReader(
        this.arrowInputPartitionHelper.getReadRowsResponses(),
        this.arrowInputPartitionHelper.getSerializedArrowSchema(),
        this.arrowInputPartitionHelper.getReadRowsHelper(),
        this.arrowInputPartitionHelper.getSelectedFields(),
        this.arrowInputPartitionHelper.getTracer(),
        this.arrowInputPartitionHelper.getUserProvidedSchema().toJavaUtil(),
        this.arrowInputPartitionHelper.getOptions().numBackgroundThreads());
  }
}
