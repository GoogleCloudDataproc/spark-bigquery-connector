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
