package com.google.cloud.spark.bigquery.custom;

import com.google.cloud.spark.bigquery.common.GenericBQDataSourceReaderHelper;
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
        GenericBQDataSourceReaderHelper dataSourceReaderHelper = new GenericBQDataSourceReaderHelper();
        return dataSourceReaderHelper.readschema(schema,table);
    }

    @Override
    public Batch toBatch() {
        return this;
    }
}
