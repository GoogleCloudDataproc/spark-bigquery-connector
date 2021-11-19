package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.common.GenericArrowColumnBatchPartitionReader;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.*;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class ArrowColumnBatchPartitionReader implements PartitionReader<ColumnarBatch> {

  //  private final Map<String, StructField> userProvidedFieldMap;
  //  private ColumnarBatch currentBatch;
  //  private boolean closed = false;
  private GenericArrowColumnBatchPartitionReader columnBatchPartitionReaderHelper;

  public ArrowColumnBatchPartitionReader(
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString schema,
      ReadRowsHelper readRowsHelper,
      List<String> namesInOrder,
      BigQueryStorageReadRowsTracer tracer,
      Optional<StructType> userProvidedSchema,
      int numBackgroundThreads) {
    this.columnBatchPartitionReaderHelper =
        new GenericArrowColumnBatchPartitionReader(
            readRowsResponses,
            schema,
            readRowsHelper,
            namesInOrder,
            tracer,
            userProvidedSchema,
            numBackgroundThreads);
  }

  @Override
  public boolean next() throws IOException {
    return this.columnBatchPartitionReaderHelper.next();
  }

  @Override
  public ColumnarBatch get() {
    return this.columnBatchPartitionReaderHelper.getCurrentBatch();
  }

  @Override
  public void close() throws IOException {
    this.columnBatchPartitionReaderHelper.close();
  }
}
