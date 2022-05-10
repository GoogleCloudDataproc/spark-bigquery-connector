package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import org.apache.spark.sql.catalyst.InternalRow;

/** Iterator on InternalRow that wraps the conversion from Avro/Arrow schema to InternalRow */
public class InternalRowIterator implements Iterator<InternalRow> {
  private Iterator<ReadRowsResponse> readRowsResponses;
  private ReadRowsResponseToInternalRowIteratorConverter converter;
  private ReadRowsHelper readRowsHelper;
  private Iterator<InternalRow> rows = ImmutableList.<InternalRow>of().iterator();

  public InternalRowIterator(
      Iterator<ReadRowsResponse> readRowsResponses,
      ReadRowsResponseToInternalRowIteratorConverter converter,
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
  public InternalRow next() {
    return rows.next();
  }
}
