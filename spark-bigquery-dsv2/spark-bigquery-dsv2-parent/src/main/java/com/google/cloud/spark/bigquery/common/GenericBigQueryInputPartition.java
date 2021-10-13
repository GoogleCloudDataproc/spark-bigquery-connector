package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;

import java.util.Iterator;


public class GenericBigQueryInputPartition {
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final String streamName;
    private final ReadRowsHelper.Options options;
    private final ReadRowsResponseToInternalRowIteratorConverter converter;
    private ReadRowsHelper readRowsHelper;


    public GenericBigQueryInputPartition(
            BigQueryReadClientFactory bigQueryReadClientFactory,
            String streamName,
            ReadRowsHelper.Options options,
            ReadRowsResponseToInternalRowIteratorConverter converter) {
        this.bigQueryReadClientFactory = bigQueryReadClientFactory;
        this.streamName = streamName;
        this.options = options;
        this.converter = converter;
    }

    public ReadRowsHelper getReadRowsHelper() {
        return readRowsHelper;
    }

    public BigQueryReadClientFactory getBigQueryReadClientFactory() {
        return bigQueryReadClientFactory;
    }

    public String getStreamName() {
        return streamName;
    }

    public ReadRowsHelper.Options getOptions() {
        return options;
    }

    public ReadRowsResponseToInternalRowIteratorConverter getConverter() {
        return converter;
    }

    // Get BigQuery Readrowsresponse object by passing the name of bigquery stream name
    public Iterator<ReadRowsResponse> getReadRowsResponse() {
        //Create Bigquery Read rows Request object by passing bigquery stream name
        ReadRowsRequest.Builder readRowsRequest =
                ReadRowsRequest.newBuilder().setReadStream(this.streamName);
        ReadRowsHelper readRowsHelper =
                new ReadRowsHelper(this.bigQueryReadClientFactory, readRowsRequest, this.options);
        this.readRowsHelper = readRowsHelper;
        Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
        return readRowsResponses;
    }
}
