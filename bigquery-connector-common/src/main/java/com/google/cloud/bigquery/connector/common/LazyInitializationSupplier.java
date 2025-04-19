/*
 * Copyright 2024 Google LLC
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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Supplier;
import javax.annotation.CheckForNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/*
 * This class is adapted from MemoizingSupplier of Google's Guava library.
 * A separate class is added to be able to access initialized variable to check if the value is already built.
 */
public class LazyInitializationSupplier<T extends @Nullable Object> implements Supplier<T> {
  final Supplier<T> delegate;
  volatile boolean initialized;
  @CheckForNull transient T value;

  public LazyInitializationSupplier(Supplier<T> delegate) {
    this.delegate = checkNotNull(delegate);
  }

  @Override
  public T get() {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          T t = delegate.get();
          value = t;
          initialized = true;
          return t;
        }
      }
    }
    return value;
  }

  @Override
  public String toString() {
    return "Suppliers.memoize("
        + (initialized ? "<supplier that returned " + value + ">" : delegate)
        + ")";
  }

  public boolean isInitialized() {
    return initialized;
  }
}
