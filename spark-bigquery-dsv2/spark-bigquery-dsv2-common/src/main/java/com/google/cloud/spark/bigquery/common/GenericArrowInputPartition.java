package com.google.cloud.spark.bigquery.common;

import static com.google.common.base.Optional.fromJavaUtil;

import com.google.cloud.bigquery.connector.common.*;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.types.StructType;

public class GenericArrowInputPartition implements Serializable {
  private final BigQueryReadClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory tracerFactory;
  private List<String> streamNames;
  private final ReadRowsHelper.Options options;
  private final ImmutableList<String> selectedFields;
  private final ByteString serializedArrowSchema;
  private final com.google.common.base.Optional<StructType> userProvidedSchema;
  private String streamName;

  public GenericArrowInputPartition(
      BigQueryReadClientFactory bigQueryReadClientFactory,
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
      BigQueryReadClientFactory bigQueryReadClientFactory,
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

  public BigQueryReadClientFactory getBigQueryReadClientFactory() {
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
}
