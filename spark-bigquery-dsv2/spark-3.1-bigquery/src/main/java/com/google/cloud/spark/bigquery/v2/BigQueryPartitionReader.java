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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.v2.context.InputPartitionReaderContext;
import java.io.IOException;
import org.apache.spark.sql.connector.read.PartitionReader;

public class BigQueryPartitionReader<T> implements PartitionReader<T> {

  private InputPartitionReaderContext<T> context;

  public BigQueryPartitionReader(InputPartitionReaderContext<T> context) {
    this.context = context;
  }

  @Override
  public boolean next() throws IOException {
    return context.next();
  }

  @Override
  public T get() {
    return context.get();
  }

  @Override
  public void close() throws IOException {
    context.close();
  }
}
