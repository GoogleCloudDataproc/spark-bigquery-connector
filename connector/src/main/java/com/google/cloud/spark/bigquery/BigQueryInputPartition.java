package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.BigQueryStorageClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.common.collect.Iterators;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.util.Iterator;

public class BigQueryInputPartition implements InputPartition<InternalRow> {

    private final BigQueryStorageClientFactory bigQueryStorageClientFactory;
    private final  String streamName;
    private final int maxReadRowsRetries;
    private final ReadRowsResponseToInternalRowIteratorConverter converter;

    public BigQueryInputPartition(
            BigQueryStorageClientFactory bigQueryStorageClientFactory,
            String streamName,
            int maxReadRowsRetries,
            ReadRowsResponseToInternalRowIteratorConverter converter) {
        this.bigQueryStorageClientFactory = bigQueryStorageClientFactory;
        this.streamName = streamName;
        this.maxReadRowsRetries = maxReadRowsRetries;
        this.converter = converter;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        Storage.ReadRowsRequest.Builder readRowsRequest = Storage.ReadRowsRequest.newBuilder()
                .setReadPosition(Storage.StreamPosition.newBuilder()
                        .setStream(Storage.Stream.newBuilder()
                                .setName(streamName)));
        ReadRowsHelper readRowsHelper = new ReadRowsHelper(bigQueryStorageClientFactory, readRowsRequest, maxReadRowsRetries);
        Iterator<Storage.ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
        return new BigQueryInputPartitionReader(readRowsResponses, converter);
    }
}
