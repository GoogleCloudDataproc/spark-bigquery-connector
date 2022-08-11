/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigquery.connector.common;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowReaderIterator implements Iterator<VectorSchemaRoot> {

  private static final Logger log = LoggerFactory.getLogger(ArrowReaderIterator.class);
  boolean closed = false;
  VectorSchemaRoot current = null;
  ArrowReader reader;

  public ArrowReaderIterator(ArrowReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean hasNext() {
    if (current != null) {
      return true;
    }

    if (closed) {
      return false;
    }

    try {
      boolean res = reader.loadNextBatch();
      if (res) {
        current = reader.getVectorSchemaRoot();
      } else {
        ensureClosed();
      }
      return res;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load the next arrow batch", e);
    }
  }

  @Override
  public VectorSchemaRoot next() {
    VectorSchemaRoot res = current;
    current = null;
    return res;
  }

  private void ensureClosed() throws IOException {
    if (!closed) {
      reader.close();
      closed = true;
    }
  }
}
