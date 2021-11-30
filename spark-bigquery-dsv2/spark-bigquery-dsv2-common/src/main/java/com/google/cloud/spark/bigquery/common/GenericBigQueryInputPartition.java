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
package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.spark.sql.catalyst.InternalRow;

public class GenericBigQueryInputPartition implements Serializable {
  private BigQueryClientFactory bigQueryReadClientFactory;
  private String streamName;
  private ReadRowsHelper.Options options;
  private ReadRowsResponseToInternalRowIteratorConverter converter;
  private ReadRowsHelper readRowsHelper;
  private int partitionSize;
  private int currentIndex;

  public GenericBigQueryInputPartition(
      BigQueryClientFactory bigQueryReadClientFactory,
      String streamName,
      ReadRowsHelper.Options options,
      ReadRowsResponseToInternalRowIteratorConverter converter) {
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.streamName = streamName;
    this.options = options;
    this.converter = converter;
  }

  public GenericBigQueryInputPartition(int partitionSize) {
    this.partitionSize = partitionSize;
    this.currentIndex = 0;
  }

  public int getCurrentIndex() {
    return currentIndex;
  }

  public ReadRowsHelper getReadRowsHelper() {
    return readRowsHelper;
  }

  public BigQueryClientFactory getBigQueryReadClientFactory() {
    return bigQueryReadClientFactory;
  }

  public String getStreamName() {
    return streamName;
  }

  public ReadRowsHelper.Options getOptions() {
    return options;
  }

  public ReadRowsResponseToInternalRowIteratorConverter getConverter() {
    return converter;
  }

  public int getPartitionSize() {
    return partitionSize;
  }

  // Get BigQuery Readrowsresponse object by passing the name of bigquery stream name
  public Iterator<ReadRowsResponse> getReadRowsResponse() {
    // Create Bigquery Read rows Request object by passing bigquery stream name (For each logical
    // bigquery stream we will have separate readRowrequest object)
    ReadRowsRequest.Builder readRowsRequest =
        ReadRowsRequest.newBuilder().setReadStream(this.streamName);

    ReadRowsHelper readRowsHelper =
        new ReadRowsHelper(this.bigQueryReadClientFactory, readRowsRequest, this.options);
    this.readRowsHelper = readRowsHelper;
    Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
    return readRowsResponses;
  }

  public boolean next() {
    return this.currentIndex < this.partitionSize;
  }

  public InternalRow get() {
    this.currentIndex++;
    return InternalRow.empty();
  }
}
