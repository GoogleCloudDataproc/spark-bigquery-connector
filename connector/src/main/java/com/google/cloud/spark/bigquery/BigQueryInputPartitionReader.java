package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.common.collect.Iterators;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.io.IOException;
import java.util.Iterator;

class BigQueryInputPartitionReader implements InputPartitionReader<InternalRow> {

     private Iterator<InternalRow> internalRowIterator;
    private InternalRow currentRow;


    BigQueryInputPartitionReader(
            Iterator<Storage.ReadRowsResponse> rows,
            ReadRowsResponseToInternalRowIteratorConverter converter) {
        this.internalRowIterator = Iterators.concat(Iterators.transform(rows, converter::convert));
    }

    @Override
    public boolean next() throws IOException {
        if (internalRowIterator.hasNext()) {
            currentRow = internalRowIterator.next();
            return true;
        }
        return false;
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {

    }
}
