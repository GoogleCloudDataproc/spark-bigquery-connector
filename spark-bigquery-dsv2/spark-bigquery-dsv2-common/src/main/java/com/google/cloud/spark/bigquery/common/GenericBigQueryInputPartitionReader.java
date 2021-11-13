package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.spark.sql.catalyst.InternalRow;

public class GenericBigQueryInputPartitionReader implements Serializable {

  private Iterator<ReadRowsResponse> readRowsResponses;
  private ReadRowsResponseToInternalRowIteratorConverter converter;
  private ReadRowsHelper readRowsHelper;
  private Iterator<InternalRow> rows = ImmutableList.<InternalRow>of().iterator();
  private InternalRow currentRow;

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

  public Iterator<InternalRow> getRows() {
    return rows;
  }

  public InternalRow getCurrentRow() {
    return currentRow;
  }

  public boolean next() {
    while (!rows.hasNext()) {
      if (!this.readRowsResponses.hasNext()) {
        return false;
      }
      ReadRowsResponse readRowsResponse = this.readRowsResponses.next();
      rows = this.converter.convert(readRowsResponse);
    }
    currentRow = rows.next();
    return true;
  }
}
