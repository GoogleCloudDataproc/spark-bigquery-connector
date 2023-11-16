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
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.UnknownFieldSet;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import org.apache.spark.sql.catalyst.InternalRow;

class BigQueryInputPartitionReaderContext implements InputPartitionReaderContext<InternalRow> {

  private Iterator<ReadRowsResponse> readRowsResponses;
  private ReadRowsResponseToInternalRowIteratorConverter converter;
  private ReadRowsHelper readRowsHelper;
  private Iterator<InternalRow> rows = ImmutableList.<InternalRow>of().iterator();
  private InternalRow currentRow;

  BigQueryInputPartitionReaderContext(
      Iterator<ReadRowsResponse> readRowsResponses,
      ReadRowsResponseToInternalRowIteratorConverter converter,
      ReadRowsHelper readRowsHelper) {
    this.readRowsResponses = readRowsResponses;
    this.converter = converter;
    this.readRowsHelper = readRowsHelper;
  }

  @Override
  public boolean next() throws IOException {
    while (!rows.hasNext()) {
      if (!readRowsResponses.hasNext()) {
        return false;
      }
      ReadRowsResponse readRowsResponse = readRowsResponses.next();
      // This is not hit.
      UnknownFieldSet unknownFieldSet = readRowsResponse.getUnknownFields();
      java.util.Map<Integer, UnknownFieldSet.Field> unknownFieldSetMap = unknownFieldSet.asMap();
      System.out.printf(
          "AQIU: BigQueryInputPartitionReaderContext ReadRowsResponse UnknownFieldSet.asMap {} \n",
          unknownFieldSetMap);
      rows = converter.convert(readRowsResponse);
    }
    currentRow = rows.next();
    return true;
  }

  @Override
  public InternalRow get() {
    return currentRow;
  }

  @Override
  public Optional<BigQueryStorageReadRowsTracer> getBigQueryStorageReadRowsTracer() {
    return Optional.empty();
  }

  @Override
  public void close() throws IOException {
    readRowsHelper.close();
  }
}
