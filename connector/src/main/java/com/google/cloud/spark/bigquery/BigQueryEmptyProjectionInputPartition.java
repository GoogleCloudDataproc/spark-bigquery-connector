package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.BigQueryStorageClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.util.Iterator;

public class BigQueryEmptyProjectionInputPartition implements InputPartition<InternalRow> {

    final int partitionSize;

    public BigQueryEmptyProjectionInputPartition(int partitionSize) {
        this.partitionSize = partitionSize;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        return new BigQueryEmptyProjectionInputPartitionReader(partitionSize);
    }
}
