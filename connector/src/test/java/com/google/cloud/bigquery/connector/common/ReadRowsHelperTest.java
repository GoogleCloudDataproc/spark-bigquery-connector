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

import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.util.Iterator;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

public class ReadRowsHelperTest {
  // it is not used, we just need the reference
  BigQueryReadClientFactory clientFactory = mock(BigQueryReadClientFactory.class);
  private ReadRowsRequest.Builder request = ReadRowsRequest.newBuilder().setReadStream("test");

  @Test
  public void testNoFailures() {
    MockResponsesBatch batch1 = new MockResponsesBatch();
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build());
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());

    // so we can run multiple tests
    ImmutableList<ReadRowsResponse> responses =
        ImmutableList.copyOf(
            new MockReadRowsHelper(clientFactory, request, 3, ImmutableList.of(batch1)).readRows());

    assertThat(responses.size()).isEqualTo(2);
    assertThat(responses.stream().mapToLong(ReadRowsResponse::getRowCount).sum()).isEqualTo(21);
  }

  @Test
  public void testRetryOfSingleFailure() {
    MockResponsesBatch batch1 = new MockResponsesBatch();
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build());
    batch1.addException(
        new StatusRuntimeException(
            Status.INTERNAL.withDescription("Received unexpected EOS on DATA frame from server.")));
    MockResponsesBatch batch2 = new MockResponsesBatch();
    batch2.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());

    ImmutableList<ReadRowsResponse> responses =
        ImmutableList.copyOf(
            new MockReadRowsHelper(clientFactory, request, 3, ImmutableList.of(batch1, batch2))
                .readRows());

    assertThat(responses.size()).isEqualTo(2);
    assertThat(responses.stream().mapToLong(ReadRowsResponse::getRowCount).sum()).isEqualTo(21);
  }

  private static final class MockReadRowsHelper extends ReadRowsHelper {
    Iterator<MockResponsesBatch> responses;

    MockReadRowsHelper(
        BigQueryReadClientFactory clientFactory,
        ReadRowsRequest.Builder request,
        int maxReadRowsRetries,
        Iterable<MockResponsesBatch> responses) {
      super(clientFactory, request, maxReadRowsRetries);
      this.responses = responses.iterator();
    }

    @Override
    protected Iterator<ReadRowsResponse> fetchResponses(ReadRowsRequest.Builder readRowsRequest) {
      return responses.next();
    }
  }
}
