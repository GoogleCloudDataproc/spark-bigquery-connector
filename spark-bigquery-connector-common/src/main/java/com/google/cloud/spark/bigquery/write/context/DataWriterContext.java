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
package com.google.cloud.spark.bigquery.write.context;

import java.io.Closeable;
import java.io.IOException;

/**
 * An internal version to Spark DataSource DataWriter interface
 *
 * @param <T>
 */
public interface DataWriterContext<T> extends Closeable {
  void write(T row) throws IOException;

  WriterCommitMessageContext commit() throws IOException;

  void abort() throws IOException;
}
