/*
 * Copyright 2026 Google LLC
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

/**
 * How ReadRows calls are assigned to the channels of the read client's gRPC channel pool, set with
 * the {@code readRowsChannelSelection} option.
 *
 * <p>{@link #LEAST_LOADED} is experimental and off by default. The option is intentionally left out
 * of the public documentation for now, and may change or be removed in a future release.
 */
public enum ReadRowsChannelSelection {
  /** The gax channel pool assigns calls to its channels round-robin. This is the default. */
  ROUND_ROBIN,
  /**
   * Each call is sent on the channel with the fewest active ReadRows streams, as picked by {@link
   * ReadRowsChannelSelector}.
   */
  LEAST_LOADED
}
