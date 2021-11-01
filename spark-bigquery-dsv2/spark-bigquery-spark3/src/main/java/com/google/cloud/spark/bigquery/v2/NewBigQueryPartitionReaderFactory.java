package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.util.Iterator;

public class NewBigQueryPartitionReaderFactory implements PartitionReaderFactory {
    private Iterator<ReadRowsResponse> readRowsResponses;
    private ReadRowsResponseToInternalRowIteratorConverter converter;
    private ReadRowsHelper readRowsHelper;
    NewBigQueryPartitionReaderFactory(
            Iterator<ReadRowsResponse> readRowsResponses,
            ReadRowsResponseToInternalRowIteratorConverter converter,
            ReadRowsHelper readRowsHelper) {
        this.readRowsResponses = readRowsResponses;
        this.converter = converter;
        this.readRowsHelper = readRowsHelper;
    }
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new NewBigQueryInputPartitionReader(readRowsResponses, converter, readRowsHelper);
    }
}
