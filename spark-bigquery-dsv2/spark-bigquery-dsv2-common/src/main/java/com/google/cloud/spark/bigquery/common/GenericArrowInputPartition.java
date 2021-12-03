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

import static com.google.common.base.Optional.fromJavaUtil;

import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.StructType;

/** Helper class to create an input partition instance for Arrow data format */
public class GenericArrowInputPartition implements Serializable {
  private final BigQueryClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory tracerFactory;
  private List<String> streamNames;
  private final ReadRowsHelper.Options options;
  private final ImmutableList<String> selectedFields;
  private final ByteString serializedArrowSchema;
  private final com.google.common.base.Optional<StructType> userProvidedSchema;
  private String streamName;
  private BigQueryStorageReadRowsTracer tracer;
  private List<ReadRowsRequest.Builder> readRowsRequests;
  private ReadRowsHelper readRowsHelper;
  private Iterator<ReadRowsResponse> readRowsResponses;

  public GenericArrowInputPartition(
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      List<String> names,
      ReadRowsHelper.Options options,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse,
      Optional<StructType> userProvidedSchema) {
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.streamNames = names;
    this.options = options;
    this.selectedFields = selectedFields;
    this.serializedArrowSchema =
        readSessionResponse.getReadSession().getArrowSchema().getSerializedSchema();
    this.tracerFactory = tracerFactory;
    this.userProvidedSchema = fromJavaUtil(userProvidedSchema);
  }

  public GenericArrowInputPartition(
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      String names,
      ReadRowsHelper.Options options,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse,
      Optional<StructType> userProvidedSchema) {
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.streamName = names;
    this.options = options;
    this.selectedFields = selectedFields;
    this.serializedArrowSchema =
        readSessionResponse.getReadSession().getArrowSchema().getSerializedSchema();
    this.tracerFactory = tracerFactory;
    this.userProvidedSchema = fromJavaUtil(userProvidedSchema);
  }

  public com.google.common.base.Optional<StructType> getUserProvidedSchema() {
    return userProvidedSchema;
  }

  public BigQueryClientFactory getBigQueryReadClientFactory() {
    return bigQueryReadClientFactory;
  }

  public BigQueryTracerFactory getTracerFactory() {
    return tracerFactory;
  }

  public List<String> getStreamNames() {
    return streamNames;
  }

  public String getStreamName() {
    return streamName;
  }

  public ReadRowsHelper.Options getOptions() {
    return options;
  }

  public ImmutableList<String> getSelectedFields() {
    return selectedFields;
  }

  public ByteString getSerializedArrowSchema() {
    return serializedArrowSchema;
  }

  public BigQueryStorageReadRowsTracer getTracer() {
    return tracer;
  }

  public List<ReadRowsRequest.Builder> getReadRowsRequests() {
    return readRowsRequests;
  }

  public ReadRowsHelper getReadRowsHelper() {
    return readRowsHelper;
  }

  public Iterator<ReadRowsResponse> getReadRowsResponses() {
    return readRowsResponses;
  }

  /**
   * Method to get list of read rows request builder from a list of stream names
   *
   * @param streamNames list of stream names
   * @return list of read rows request builder
   */
  public List<ReadRowsRequest.Builder> getListOfReadRowsRequestsByStreamNames(
      List<String> streamNames) {
    List<ReadRowsRequest.Builder> readRowsRequests =
        streamNames.stream()
            .map(name -> ReadRowsRequest.newBuilder().setReadStream(name))
            .collect(Collectors.toList());
    return readRowsRequests;
  }

  /**
   * Method to get list of read rows request builder from a stream name
   *
   * @param streamName name of the stream
   * @return list of read rows request builder
   */
  public List<ReadRowsRequest.Builder> getListOfReadRowsRequestsByStreamNames(String streamName) {
    ReadRowsRequest.Builder readRowsRequest =
        ReadRowsRequest.newBuilder().setReadStream(streamName);
    List<ReadRowsRequest.Builder> readRowsRequests = new ArrayList<ReadRowsRequest.Builder>();
    readRowsRequests.add(readRowsRequest);
    return readRowsRequests;
  }

  /**
   * Method to create a bigquery storage read rows tracer
   *
   * @param tracerFactory create application level tracers for read operation
   * @param streamNames list of stream names
   * @return
   */
  public BigQueryStorageReadRowsTracer getBQTracerByStreamNames(
      BigQueryTracerFactory tracerFactory, List<String> streamNames) {
    return tracerFactory.newReadRowsTracer(Joiner.on(",").join(streamNames));
  }

  /**
   * Method to create a bigquery storage read rows tracer
   *
   * @param tracerFactory create application level tracers for read operation
   * @param streamName name of the stream
   * @return
   */
  public BigQueryStorageReadRowsTracer getBQTracerByStreamNames(
      BigQueryTracerFactory tracerFactory, String streamName) {
    return tracerFactory.newReadRowsTracer(streamName);
  }

  /** Method to enable creation of partition reader with stream name */
  public void createPartitionReaderByName() {
    // using generic helper class from dsv 2 parent library to create tracer,read row request object
    //  for each inputPartition reader
    this.tracer = this.getBQTracerByStreamNames(this.tracerFactory, this.streamName);
    this.readRowsRequests = this.getListOfReadRowsRequestsByStreamNames(this.streamName);
    this.readRowsHelper =
        new ReadRowsHelper(this.bigQueryReadClientFactory, readRowsRequests, this.options);
    tracer.startStream();
    // iterator to read data from bigquery read rows object
    this.readRowsResponses = this.readRowsHelper.readRows();
  }

  /** Method to enable creation of partition reader with a list stream names */
  public void createPartitionReader() {
    // using generic helper class from dsv 2 parent library to create tracer,read row request object
    //  for each inputPartition reader
    this.tracer = this.getBQTracerByStreamNames(this.tracerFactory, this.streamNames);
    this.readRowsRequests = this.getListOfReadRowsRequestsByStreamNames(this.streamNames);

    this.readRowsHelper =
        new ReadRowsHelper(this.bigQueryReadClientFactory, readRowsRequests, this.options);
    tracer.startStream();
    // iterator to read data from bigquery read rows object
    this.readRowsResponses = this.readRowsHelper.readRows();
  }
}
