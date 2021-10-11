package com.google.cloud.spark.bigquery.custom;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.*;


public class CustomScanWithBatch implements Scan, Batch {

    @Override
    public InputPartition[] planInputPartitions() {
        return new InputPartition[]{new CustomInputPartition()};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new CustomPartitionReaderFactory();
    }

    @Override
    public StructType readSchema() {
        StructField[] structFields = new StructField[]{
                new StructField("value", DataTypes.StringType, true, Metadata.empty())};
        return new StructType(structFields);
    }

    @Override
    public Batch toBatch() {
        return this;
    }
}
