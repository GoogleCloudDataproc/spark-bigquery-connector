package com.google.cloud.spark.bigquery;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.io.IOException;

class BigQueryEmptyProjectionInputPartitionReader implements InputPartitionReader<InternalRow> {

    final int partitionSize;
    int currentIndex;

    BigQueryEmptyProjectionInputPartitionReader(int partitionSize) {
        this.partitionSize = partitionSize;
        this.currentIndex = 0;
    }

    @Override
    public boolean next() throws IOException {
        return currentIndex < partitionSize;
    }

    @Override
    public InternalRow get() {
        currentIndex++;
        return InternalRow.empty();
    }

    @Override
    public void close() throws IOException {
        // empty
    }
}
