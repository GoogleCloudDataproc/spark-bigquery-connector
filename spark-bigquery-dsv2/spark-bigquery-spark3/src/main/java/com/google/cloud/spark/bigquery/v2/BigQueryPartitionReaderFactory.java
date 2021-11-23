package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.common.GenericArrowBigQueryInputPartitionHelper;
import java.util.Iterator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class BigQueryPartitionReaderFactory implements PartitionReaderFactory {

  BigQueryPartitionReaderFactory() {}

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    if (partition instanceof BigQueryInputPartition) {
      ReadRowsRequest.Builder readRowsRequest =
          ReadRowsRequest.newBuilder()
              .setReadStream(((BigQueryInputPartition) partition).getStreamName());
      ReadRowsHelper readRowsHelper =
          new ReadRowsHelper(
              ((BigQueryInputPartition) partition).getBigQueryReadClientFactory(),
              readRowsRequest,
              ((BigQueryInputPartition) partition).getOptions());
      Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
      return new BigQueryInputPartitionReader(
          readRowsResponses, ((BigQueryInputPartition) partition).getConverter(), readRowsHelper);
    } else if (partition instanceof BigQueryEmptyProjectInputPartition) {
      return new BigQueryEmptyProjectionInputPartitionReader(
          ((BigQueryEmptyProjectInputPartition) partition).getPartitionSize());
    } else {
      throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
    }
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    if (partition instanceof ArrowInputPartition) {
      ArrowInputPartition arrowInputPartition = (ArrowInputPartition) partition;
      GenericArrowBigQueryInputPartitionHelper bqInputPartitionHelper =
          new GenericArrowBigQueryInputPartitionHelper();
      arrowInputPartition.createPartitionReader();
      return new ArrowColumnBatchPartitionReader(
          arrowInputPartition.getReadRowsResponses(),
          arrowInputPartition.getSerializedArrowSchema(),
          arrowInputPartition.getReadRowsHelper(),
          arrowInputPartition.getSelectedFields(),
          arrowInputPartition.getTracer(),
          arrowInputPartition.getUserProvidedSchema().toJavaUtil(),
          arrowInputPartition.getOptions().numBackgroundThreads());

    } else {
      throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
    }
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    if (partition instanceof ArrowInputPartition) {
      return true;
    }
    return false;
  }
}
