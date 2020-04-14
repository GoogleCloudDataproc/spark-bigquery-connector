package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.util.Iterator;

public class BigQueryInputPartition implements InputPartition<InternalRow> {

    private final  BigQueryStorageClient client;
    private final  String streamName;
    private final int maxReadRowsRetries;
    private final ReadRowsResponseToInternalRowIteratorConverter converter;

    public BigQueryInputPartition(
            BigQueryStorageClient client,
            String streamName,
            int maxReadRowsRetries,
            ReadRowsResponseToInternalRowIteratorConverter converter) {
        this.client = client;
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
        ReadRowsHelper readRowsHelper = new ReadRowsHelper(client, readRowsRequest, maxReadRowsRetries);
        Iterator<Storage.ReadRowsResponse> rows = readRowsHelper.readRows();
        return new BigQueryInputPartitionReader(rows, converter, client);
    }
}
