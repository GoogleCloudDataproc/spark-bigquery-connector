package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ArrowSchemaConverter;
import com.google.cloud.spark.bigquery.common.GenericArrowColumnBatchPartitionReader;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class ArrowColumnBatchPartitionReader extends GenericArrowColumnBatchPartitionReader
    implements PartitionReader<ColumnarBatch> {

  private final Map<String, StructField> userProvidedFieldMap;
  private ColumnarBatch currentBatch;
  private boolean closed = false;

  public ArrowColumnBatchPartitionReader(
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString schema,
      ReadRowsHelper readRowsHelper,
      List<String> namesInOrder,
      BigQueryStorageReadRowsTracer tracer,
      Optional<StructType> userProvidedSchema,
      int numBackgroundThreads) {
    super(
        readRowsResponses,
        schema,
        readRowsHelper,
        namesInOrder,
        tracer,
        userProvidedSchema,
        numBackgroundThreads);
    List<StructField> userProvidedFieldList =
        Arrays.stream(userProvidedSchema.orElse(new StructType()).fields())
            .collect(Collectors.toList());
    this.userProvidedFieldMap =
        userProvidedFieldList.stream().collect(Collectors.toMap(StructField::name, field -> field));
  }

  @Override
  public boolean next() throws IOException {
    super.getTracer().nextBatchNeeded();
    if (closed) {
      return false;
    }
    super.getTracer().rowsParseStarted();

    closed = !super.getReader().loadNextBatch();

    if (closed) {
      return false;
    }

    VectorSchemaRoot root = super.getReader().root();
    if (currentBatch == null) {
      // trying to verify from dev@spark but this object
      // should only need to get created once.  The underlying
      // vectors should stay the same.
      ColumnVector[] columns =
          super.getNamesInOrder().stream()
              .map(root::getVector)
              .map(
                  vector ->
                      new ArrowSchemaConverter(vector, userProvidedFieldMap.get(vector.getName())))
              .toArray(ColumnVector[]::new);

      currentBatch = new ColumnarBatch(columns);
    }
    currentBatch.setNumRows(root.getRowCount());
    super.getTracer().rowsParseFinished(currentBatch.numRows());
    return true;
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    try {
      super.getTracer().finished();
      super.getCloseables().set(0, super.getReader());
      super.getCloseables().add(super.getAllocator());
      AutoCloseables.close(super.getCloseables());
    } catch (Exception e) {
      throw new IOException(
          "Failure closing arrow components. stream: " + super.getReadRowsHelper(), e);
    } finally {
      try {
        super.getReadRowsHelper().close();
      } catch (Exception e) {
        throw new IOException("Failure closing stream: " + super.getReadRowsHelper(), e);
      }
    }
  }
}
