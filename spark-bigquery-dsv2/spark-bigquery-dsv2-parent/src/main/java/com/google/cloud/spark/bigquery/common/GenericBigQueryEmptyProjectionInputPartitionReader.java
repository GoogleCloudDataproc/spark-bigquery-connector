package com.google.cloud.spark.bigquery.common;

public class GenericBigQueryEmptyProjectionInputPartitionReader {

  public final int partitionSize;
  public int currentIndex;

  public GenericBigQueryEmptyProjectionInputPartitionReader(int partitionSize) {
    this.partitionSize = partitionSize;
    this.currentIndex = 0;
  }
}
