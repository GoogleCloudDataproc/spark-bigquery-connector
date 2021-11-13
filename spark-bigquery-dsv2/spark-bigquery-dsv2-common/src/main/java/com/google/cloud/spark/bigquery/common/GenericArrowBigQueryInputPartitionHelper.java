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
