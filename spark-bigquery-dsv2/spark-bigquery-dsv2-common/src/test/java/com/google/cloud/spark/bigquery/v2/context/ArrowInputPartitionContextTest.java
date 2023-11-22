/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2.context;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Optional;
import org.junit.Test;

public class ArrowInputPartitionContextTest {
  @Test
  public void testSerializability() throws IOException {
    new ObjectOutputStream(new ByteArrayOutputStream())
        .writeObject(
            new ArrowInputPartitionContext(
                /*bigQueryClientFactory=*/ null,
                /*tracerFactory=*/ null,
                Lists.newArrayList("streamName"),
                new ReadRowsHelper.Options(
                    /*maxRetries=*/ 5,
                    Optional.of("endpoint"),
                    /*backgroundParsingThreads=*/ 5,
                    /*prebufferResponses=*/ 1),
                null,
                new ReadSessionResponse(ReadSession.getDefaultInstance(), null),
                null, null));
  }
}
