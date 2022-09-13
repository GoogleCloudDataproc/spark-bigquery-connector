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

import org.apache.arrow.memory.NettyAllocationManager;
import org.apache.arrow.memory.RootAllocator;

/** Common utility classes for Arrow. */
public class ArrowUtil {
  private ArrowUtil() {};

  /** Returns a new allocator limited to maxAllocation bytes */
  public static RootAllocator newRootAllocator(long maxAllocation) {
    // Arrow has classpath scanning logic based on hard-coding strings to
    // select the default allocationManagerFactory by selecting one
    // here it should be properly shaded with the rest of Arrow within
    // the connector.
    return new RootAllocator(
        RootAllocator.configBuilder()
            .allocationManagerFactory(NettyAllocationManager.FACTORY)
            .maxAllocation(maxAllocation)
            .build());
  }
}
