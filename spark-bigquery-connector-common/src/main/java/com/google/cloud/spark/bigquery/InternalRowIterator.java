package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import org.apache.spark.sql.Row;

public class InternalRowIterator implements Iterator<Row> {
  private Iterator<ReadRowsResponse> readRowsResponses;
  private ReadRowsResponseToRowIteratorConverter converter;
  private ReadRowsHelper readRowsHelper;
  private Iterator<Row> rows = ImmutableList.<Row>of().iterator();

  public InternalRowIterator(
      Iterator<ReadRowsResponse> readRowsResponses,
      ReadRowsResponseToRowIteratorConverter converter,
      ReadRowsHelper readRowsHelper) {
    this.readRowsResponses = readRowsResponses;
    this.converter = converter;
    this.readRowsHelper = readRowsHelper;
  }

  @Override
  public boolean hasNext() {
    while (!rows.hasNext()) {
      if (!readRowsResponses.hasNext()) {
        readRowsHelper.close();
        return false;
      }
      ReadRowsResponse readRowsResponse = readRowsResponses.next();
      rows = converter.convert(readRowsResponse);
    }

    return true;
  }

  @Override
  public Row next() {
    return rows.next();
  }
}
