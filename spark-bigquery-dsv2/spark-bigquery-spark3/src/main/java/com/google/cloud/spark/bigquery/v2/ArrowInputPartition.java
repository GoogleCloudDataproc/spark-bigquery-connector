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

  private com.google.common.base.Optional<StructType> userProvidedSchema;

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
