package com.google.cloud.bigquery.connector.common;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.NettyAllocationManager;

/** Common utility classes for Arrow. */
public class ArrowUtil {
  private ArrowUtil() {};

  public static RootAllocator newRootAllocator(long maxAllocation) {

    return RootAllocator(
        RootAllocator.configBuilder()
            .allocationManagerFactory(new NettyAllocationManager())
            .maxAllocation(maxAllocation)
            .build());
  }
}
