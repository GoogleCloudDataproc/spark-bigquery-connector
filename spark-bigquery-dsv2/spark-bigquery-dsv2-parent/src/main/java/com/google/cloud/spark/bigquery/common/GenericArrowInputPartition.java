package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.List;

public class GenericArrowInputPartition {
  private final BigQueryReadClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory tracerFactory;
  private final List<String> streamNames;
  private final ReadRowsHelper.Options options;
  private final ImmutableList<String> selectedFields;
  private final ByteString serializedArrowSchema;

  public GenericArrowInputPartition(
      BigQueryReadClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      List<String> names,
      ReadRowsHelper.Options options,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse) {
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.streamNames = names;
    this.options = options;
    this.selectedFields = selectedFields;
    this.serializedArrowSchema =
        readSessionResponse.getReadSession().getArrowSchema().getSerializedSchema();
    this.tracerFactory = tracerFactory;
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
