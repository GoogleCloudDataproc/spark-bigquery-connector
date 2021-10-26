package com.google.cloud.spark.bigquery.common;

import java.io.Serializable;

public class GenericBigQueryEmptyProjectionInputPartition implements Serializable {
  final int partitionSize;

  public GenericBigQueryEmptyProjectionInputPartition(int partitionSize) {
    this.partitionSize = partitionSize;
  }

  public int getPartitionSize() {
    return partitionSize;
  }
}
