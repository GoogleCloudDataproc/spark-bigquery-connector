package com.google.cloud.spark.bigquery.common;

import java.io.Serializable;

public class GenericBigQueryEmptyProjectionInputPartitionReader implements Serializable {

  public final int partitionSize;
  public int currentIndex;

  public GenericBigQueryEmptyProjectionInputPartitionReader(int partitionSize) {
    this.partitionSize = partitionSize;
    this.currentIndex = 0;
  }
}
