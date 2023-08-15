package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.v2.context.InputPartitionContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class Spark32BigQueryPartitionReaderFactory implements PartitionReaderFactory {

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    InputPartitionContext<InternalRow> ctx = ((BigQueryInputPartition) partition).getContext();
    return new Spark32BigQueryPartitionReader<>(ctx.createPartitionReaderContext());
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    InputPartitionContext<ColumnarBatch> ctx = ((BigQueryInputPartition) partition).getContext();
    return new Spark32BigQueryPartitionReader<>(ctx.createPartitionReaderContext());
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    InputPartitionContext ctx = ((BigQueryInputPartition) partition).getContext();
    return ctx.supportColumnarReads();
  }
}
