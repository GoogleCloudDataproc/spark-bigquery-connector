/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsUtils {

  /** Converts HDFS RemoteIterator to java.util.Iterator */
  public static <T> Iterator<T> toJavaUtilIterator(final RemoteIterator<T> remoteIterator) {
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        try {
          return remoteIterator.hasNext();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public T next() {
        try {
          return remoteIterator.next();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    };
  }

  public static <T> Iterable<T> toJavaUtilIterable(final RemoteIterator<T> remoteIterator) {
    return () -> toJavaUtilIterator(remoteIterator);
  }
}
