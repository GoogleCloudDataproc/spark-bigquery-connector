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

import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.common.GenericArrowInputPartition;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

public class ArrowInputPartition implements InputPartition {
  public com.google.common.base.Optional<StructType> getUserProvidedSchema() {
    return this.arrowInputPartitionHelper.getUserProvidedSchema();
  }

  private GenericArrowInputPartition arrowInputPartitionHelper;

  public ArrowInputPartition(
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      String name,
      ReadRowsHelper.Options options,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse,
      Optional<StructType> userProvidedSchema) {
    this.arrowInputPartitionHelper =
        new GenericArrowInputPartition(
            bigQueryReadClientFactory,
            tracerFactory,
            name,
            options,
            selectedFields,
            readSessionResponse,
            userProvidedSchema);
  }

  public BigQueryTracerFactory getTracerFactory() {
    return this.arrowInputPartitionHelper.getTracerFactory();
  }

  public String getStreamName() {
    return this.arrowInputPartitionHelper.getStreamName();
  }

  public BigQueryClientFactory getBigQueryReadClientFactory() {
    return this.arrowInputPartitionHelper.getBigQueryReadClientFactory();
  }

  public ReadRowsHelper.Options getOptions() {
    return this.arrowInputPartitionHelper.getOptions();
  }

  public ByteString getSerializedArrowSchema() {
    return this.arrowInputPartitionHelper.getSerializedArrowSchema();
  }

  public List<String> getSelectedFields() {
    return this.arrowInputPartitionHelper.getSelectedFields();
  }

  public void createPartitionReader() {
    this.arrowInputPartitionHelper.createPartitionReaderByName();
  }

  public Iterator<ReadRowsResponse> getReadRowsResponses() {
    return this.arrowInputPartitionHelper.getReadRowsResponses();
  }

  public ReadRowsHelper getReadRowsHelper() {
    return this.arrowInputPartitionHelper.getReadRowsHelper();
  }

  public BigQueryStorageReadRowsTracer getTracer() {
    return this.arrowInputPartitionHelper.getTracer();
  }
}
