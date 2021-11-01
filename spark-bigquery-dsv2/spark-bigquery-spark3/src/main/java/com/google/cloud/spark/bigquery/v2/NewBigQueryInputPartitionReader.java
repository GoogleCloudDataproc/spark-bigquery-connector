package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.common.GenericBigQueryInputPartitionReader;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;
import java.util.Iterator;

public class NewBigQueryInputPartitionReader extends GenericBigQueryInputPartitionReader  implements PartitionReader<InternalRow> {
    private Iterator<InternalRow> rows = ImmutableList.<InternalRow>of().iterator();
    private InternalRow currentRow;

    public NewBigQueryInputPartitionReader(
            Iterator<ReadRowsResponse> readRowsResponses,
            ReadRowsResponseToInternalRowIteratorConverter converter,
            ReadRowsHelper readRowsHelper) {
        super(readRowsResponses, converter, readRowsHelper);
    }
    @Override
    public boolean next() throws IOException {
        while (!rows.hasNext()) {
            if (!super.getReadRowsResponses().hasNext()) {
                return false;
            }
            ReadRowsResponse readRowsResponse = super.getReadRowsResponses().next();
            rows = super.getConverter().convert(readRowsResponse);
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
        super.getReadRowsHelper().close();
    }
}
