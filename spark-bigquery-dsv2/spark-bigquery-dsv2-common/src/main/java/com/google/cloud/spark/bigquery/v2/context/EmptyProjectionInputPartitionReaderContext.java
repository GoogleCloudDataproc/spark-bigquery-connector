/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2.context;

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import java.io.IOException;
import java.util.Optional;
import org.apache.spark.sql.catalyst.InternalRow;

class EmptyProjectionInputPartitionReaderContext
    implements InputPartitionReaderContext<InternalRow> {

  final int partitionSize;
  int currentIndex;

  EmptyProjectionInputPartitionReaderContext(int partitionSize) {
    this.partitionSize = partitionSize;
    this.currentIndex = 0;
  }

  @Override
  public boolean next() throws IOException {
    return currentIndex < partitionSize;
  }

  @Override
  public InternalRow get() {
    currentIndex++;
    return InternalRow.empty();
  }

  @Override
  public Optional<BigQueryStorageReadRowsTracer> getBigQueryStorageReadRowsTracer() {
    return Optional.empty();
  }

  @Override
  public void close() throws IOException {
    // empty
  }
}
