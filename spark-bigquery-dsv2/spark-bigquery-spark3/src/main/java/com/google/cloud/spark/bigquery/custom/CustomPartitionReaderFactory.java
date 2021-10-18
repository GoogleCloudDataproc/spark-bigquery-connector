package com.google.cloud.spark.bigquery.custom;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class CustomPartitionReaderFactory implements PartitionReaderFactory {
  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    return new CustomPartitionReader();
  }
}
