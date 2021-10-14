package com.google.cloud.spark.bigquery.common;

public class GenericBigQueryEmptyProjectionInputPartition {
    final int partitionSize;

    public GenericBigQueryEmptyProjectionInputPartition(int partitionSize) {
        this.partitionSize = partitionSize;
    }

    public int getPartitionSize() {
        return partitionSize;
    }
}
