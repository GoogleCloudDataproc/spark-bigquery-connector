/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.StreamPosition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

public class ReadRowsHelper {
    private BigQueryStorageClient client;
    private ReadRowsRequest.Builder request;
    private int maxReadRowsRetries;

    public ReadRowsHelper(BigQueryStorageClient client, ReadRowsRequest.Builder request, int maxReadRowsRetries) {
        this.client = requireNonNull(client, "client cannot be null");
        this.request = requireNonNull(request, "client cannot be null");
        this.maxReadRowsRetries = maxReadRowsRetries;
    }

    public Iterator<ReadRowsResponse> readRows() {
        List<ReadRowsResponse> readRowResponses = new ArrayList<>();
        long readRowsCount = 0;
        int retries = 0;
        Iterator<ReadRowsResponse> serverResponses = fetchResponses(request);
        while (serverResponses.hasNext()) {
            try {
                ReadRowsResponse response = serverResponses.next();
                readRowsCount += response.getRowCount();
                readRowResponses.add(response);
            } catch (RuntimeException e) {
                // if relevant, retry the read, from the last read position
                if (BigQueryUtil.isRetryable(e) && retries < maxReadRowsRetries) {
                    request.getReadPositionBuilder().setOffset(readRowsCount);
                    serverResponses = fetchResponses(request);
                    retries++;
                } else {
                    // to safely close the client
                    try (BigQueryStorageClient ignored = client) {
                        throw e;
                    }
                }
            }
        }
        return readRowResponses.iterator();
    }

    // In order to enable testing
    protected Iterator<ReadRowsResponse> fetchResponses(ReadRowsRequest.Builder readRowsRequest) {
        return client.readRowsCallable()
                .call(readRowsRequest.build())
                .iterator();
    }

    // Ported from https://github.com/GoogleCloudDataproc/spark-bigquery-connector/pull/150
    static class ReadRowsIterator implements Iterator<ReadRowsResponse> {
        ReadRowsHelper helper;
        StreamPosition.Builder readPosition;
        Iterator<ReadRowsResponse> serverResponses;
        long readRowsCount;
        int retries;

        public ReadRowsIterator(ReadRowsHelper helper, StreamPosition.Builder readPosition, Iterator<ReadRowsResponse> serverResponses) {
            this.helper = helper;
            this.readPosition = readPosition;
            this.serverResponses = serverResponses;
        }

        @Override
        public boolean hasNext() {
            return serverResponses.hasNext();
        }

        @Override
        public ReadRowsResponse next() {
            do {
                try {
                    ReadRowsResponse response = serverResponses.next();
                    readRowsCount += response.getRowCount();
                    //logDebug(s"read ${response.getSerializedSize} bytes");
                    return response;
                } catch (Exception e ) {
                        // if relevant, retry the read, from the last read position
                        if (BigQueryUtil.isRetryable(e) && retries < helper.maxReadRowsRetries) {
                            serverResponses = helper.fetchResponses(helper.request.setReadPosition(
                                    readPosition.setOffset(readRowsCount)));
                            retries ++;
                        } else {
                            helper.client.close();
                            throw e;
                        }
                }
            } while (serverResponses.hasNext());

            throw new NoSuchElementException("No more server responses");
        }
    }
}
