package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

class BigQueryInputPartitionReader implements InputPartitionReader<InternalRow> {

    private Iterator<Storage.ReadRowsResponse> readRowsResponses;
    private ReadRowsResponseToInternalRowIteratorConverter converter;
    private Iterator<InternalRow> rows = ImmutableList.<InternalRow>of().iterator();
    private InternalRow currentRow;

    BigQueryInputPartitionReader(
            Iterator<Storage.ReadRowsResponse> readRowsResponses,
            ReadRowsResponseToInternalRowIteratorConverter converter) {
        this.readRowsResponses = readRowsResponses;
        this.converter = converter;
    }

    @Override
    public boolean next() throws IOException {
        while(!rows.hasNext()) {
            if(!readRowsResponses.hasNext()) {
                return false;
            }
            Storage.ReadRowsResponse readRowsResponse = readRowsResponses.next();
            rows = converter.convert(readRowsResponse);
        }
        currentRow = rows.next();
        return true;
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {

    }
}
