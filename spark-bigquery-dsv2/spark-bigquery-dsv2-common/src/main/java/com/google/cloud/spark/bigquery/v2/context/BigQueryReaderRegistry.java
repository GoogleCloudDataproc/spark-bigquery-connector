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

/**
 * Global registry acting as a Singleton accessor for the active {@link BigQueryReaderFactoryHook}.
 * Enables dynamic extension of partition reader context instantiation at JVM startup. Defaults to
 * {@link DefaultBigQueryReaderFactoryHook}.
 */
public class BigQueryReaderRegistry {
  private static final BigQueryReaderFactoryHook DEFAULT = new DefaultBigQueryReaderFactoryHook();
  private static volatile BigQueryReaderFactoryHook activeHook = DEFAULT;

  /**
   * Registers a pluggable hook factory. Passing {@code null} resets to the default hook.
   *
   * @param hook the factory hook to register, or {@code null} to reset to default
   */
  public static void register(BigQueryReaderFactoryHook hook) {
    activeHook = (hook != null) ? hook : DEFAULT;
  }

  /**
   * Gets the currently active factory hook.
   *
   * @return the active hook
   */
  public static BigQueryReaderFactoryHook get() {
    return activeHook;
  }
}
