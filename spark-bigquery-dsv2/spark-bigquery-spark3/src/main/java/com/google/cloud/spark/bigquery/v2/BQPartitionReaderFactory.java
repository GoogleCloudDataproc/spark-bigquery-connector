package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class BQPartitionReaderFactory implements PartitionReaderFactory {
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new BQPartitionReader();
    }
}
