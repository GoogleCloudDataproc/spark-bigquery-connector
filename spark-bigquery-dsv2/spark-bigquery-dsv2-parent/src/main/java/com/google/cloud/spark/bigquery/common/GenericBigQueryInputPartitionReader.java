package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import java.util.Iterator;

public class GenericBigQueryInputPartitionReader {

  private Iterator<ReadRowsResponse> readRowsResponses;
  private ReadRowsResponseToInternalRowIteratorConverter converter;
  private ReadRowsHelper readRowsHelper;

  public GenericBigQueryInputPartitionReader(
      Iterator<ReadRowsResponse> readRowsResponses,
      ReadRowsResponseToInternalRowIteratorConverter converter,
      ReadRowsHelper readRowsHelper) {
    this.readRowsResponses = readRowsResponses;
    this.converter = converter;
    this.readRowsHelper = readRowsHelper;
  }

  public Iterator<ReadRowsResponse> getReadRowsResponses() {
    return readRowsResponses;
  }

  public ReadRowsResponseToInternalRowIteratorConverter getConverter() {
    return converter;
  }

  public ReadRowsHelper getReadRowsHelper() {
    return readRowsHelper;
  }
}
