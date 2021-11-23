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

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.common.base.Joiner;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

// Utility class
public class GenericArrowBigQueryInputPartitionHelper implements Serializable {

  private BigQueryStorageReadRowsTracer tracer;

  public List<ReadRowsRequest.Builder> getListOfReadRowsRequestsByStreamNames(
      List<String> streamNames) {
    List<ReadRowsRequest.Builder> readRowsRequests =
        streamNames.stream()
            .map(name -> ReadRowsRequest.newBuilder().setReadStream(name))
            .collect(Collectors.toList());
    return readRowsRequests;
  }

  public List<ReadRowsRequest.Builder> getListOfReadRowsRequestsByStreamNames(String streamName) {
    //    List<ReadRowsRequest.Builder> readRowsRequests =
    //            streamNames.stream()
    //                    .map(name -> ReadRowsRequest.newBuilder().setReadStream(name))
    //                    .collect(Collectors.toList());
    ReadRowsRequest.Builder readRowsRequest =
        ReadRowsRequest.newBuilder().setReadStream(streamName);
    List<ReadRowsRequest.Builder> readRowsRequests = new ArrayList<ReadRowsRequest.Builder>();
    readRowsRequests.add(readRowsRequest);
    return readRowsRequests;
  }

  public BigQueryStorageReadRowsTracer getBQTracerByStreamNames(
      BigQueryTracerFactory tracerFactory, List<String> streamNames) {
    return tracerFactory.newReadRowsTracer(Joiner.on(",").join(streamNames));
  }

  public BigQueryStorageReadRowsTracer getBQTracerByStreamNames(
      BigQueryTracerFactory tracerFactory, String streamName) {
    return tracerFactory.newReadRowsTracer(streamName);
  }
}
