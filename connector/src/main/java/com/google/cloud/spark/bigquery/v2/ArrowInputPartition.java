package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class ArrowInputPartition implements InputPartition<ColumnarBatch> {

  private final BigQueryReadClientFactory bigQueryReadClientFactory;
  private final String streamName;
  private final int maxReadRowsRetries;
  private final ImmutableList<String> selectedFields;
  private final ByteString serializedArrowSchema;

  public ArrowInputPartition(
      BigQueryReadClientFactory bigQueryReadClientFactory,
      String name,
      int maxReadRowsRetries,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse) {
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.streamName = name;
    this.maxReadRowsRetries = maxReadRowsRetries;
    this.selectedFields = selectedFields;
    this.serializedArrowSchema =
        readSessionResponse.getReadSession().getArrowSchema().getSerializedSchema();
  }

  @Override
  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    ReadRowsRequest.Builder readRowsRequest =
        ReadRowsRequest.newBuilder().setReadStream(streamName);
    ReadRowsHelper readRowsHelper =
        new ReadRowsHelper(bigQueryReadClientFactory, readRowsRequest, maxReadRowsRetries);
    Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
    return new ArrowColumnBatchPartitionColumnBatchReader(
        readRowsResponses, serializedArrowSchema, readRowsHelper, selectedFields);
  }
}
