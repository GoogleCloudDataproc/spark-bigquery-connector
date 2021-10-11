package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Iterator;

public class GenericBigQueryInputPartitionReader {

    private Iterator<ReadRowsResponse> readRowsResponses;
    private ReadRowsResponseToInternalRowIteratorConverter converter;
    private ReadRowsHelper readRowsHelper;
    private Iterator<InternalRow> rows = ImmutableList.<InternalRow>of().iterator();
    private InternalRow currentRow;

    public GenericBigQueryInputPartitionReader(Iterator<ReadRowsResponse> readRowsResponses, ReadRowsResponseToInternalRowIteratorConverter converter, ReadRowsHelper readRowsHelper, Iterator<InternalRow> rows, InternalRow currentRow) {
        this.readRowsResponses = readRowsResponses;
        this.converter = converter;
        this.readRowsHelper = readRowsHelper;
        this.rows = rows;
        this.currentRow = currentRow;
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


}
