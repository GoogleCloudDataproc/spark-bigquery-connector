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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.util.Iterator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class BigQueryPartitionReaderFactory implements PartitionReaderFactory {

  BigQueryPartitionReaderFactory() {}

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    if (partition instanceof BigQueryInputPartition) {
      ReadRowsRequest.Builder readRowsRequest =
          ReadRowsRequest.newBuilder()
              .setReadStream(((BigQueryInputPartition) partition).getStreamName());
      ReadRowsHelper readRowsHelper =
          new ReadRowsHelper(
              ((BigQueryInputPartition) partition).getBigQueryReadClientFactory(),
              readRowsRequest,
              ((BigQueryInputPartition) partition).getOptions());
      Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
      return new BigQueryInputPartitionReader(
          readRowsResponses, ((BigQueryInputPartition) partition).getConverter(), readRowsHelper);
    } else if (partition instanceof BigQueryEmptyProjectInputPartition) {
      return new BigQueryEmptyProjectionInputPartitionReader(
          ((BigQueryEmptyProjectInputPartition) partition).getPartitionSize());
    } else {
      throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
    }
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    if (partition instanceof ArrowInputPartition) {
      ArrowInputPartition arrowInputPartition = (ArrowInputPartition) partition;
      arrowInputPartition.createPartitionReader();
      return new ArrowColumnBatchPartitionReader(
          arrowInputPartition.getReadRowsResponses(),
          arrowInputPartition.getSerializedArrowSchema(),
          arrowInputPartition.getReadRowsHelper(),
          arrowInputPartition.getSelectedFields(),
          arrowInputPartition.getTracer(),
          arrowInputPartition.getUserProvidedSchema().toJavaUtil(),
          arrowInputPartition.getOptions().numBackgroundThreads());

    } else {
      throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
    }
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    if (partition instanceof ArrowInputPartition) {
      return true;
    }
    return false;
  }
}
