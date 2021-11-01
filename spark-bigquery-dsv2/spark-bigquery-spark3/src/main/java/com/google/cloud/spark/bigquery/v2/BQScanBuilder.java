package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;

public class BQScanBuilder implements ScanBuilder,Scan, Batch {

    @Override
    public Scan build() {
        return this;
    }
    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return new InputPartition[] {new BQInputPartition()};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new BQPartitionReaderFactory();
    }

    @Override
    public StructType readSchema() {
        return null;
    }

}
