package com.google.cloud.spark.bigquery;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

public class ArrowBinaryIterator implements Iterator<InternalRow> {

    private static long maxAllocation = Long.MAX_VALUE;
    ArrowReaderIterator arrowReaderIterator;
    Iterator<InternalRow> currentIterator;
    List<String> columnsInOrder;

    public ArrowBinaryIterator(List<String> columnsInOrder, ByteString schema, ByteString rowsInBytes) {
        BufferAllocator allocator = (new RootAllocator(maxAllocation)).newChildAllocator("ArrowBinaryIterator",
                0, maxAllocation);

        SequenceInputStream bytesWithSchemaStream = new SequenceInputStream(
                new ByteArrayInputStream(schema.toByteArray()),
                new ByteArrayInputStream(rowsInBytes.toByteArray()));

        ArrowStreamReader arrowStreamReader = new ArrowStreamReader(bytesWithSchemaStream, allocator);
        arrowReaderIterator = new ArrowReaderIterator(arrowStreamReader);
        currentIterator = ImmutableList.<InternalRow>of().iterator();
        this.columnsInOrder = columnsInOrder;
    }

    @Override
    public boolean hasNext() {
        while (!currentIterator.hasNext()) {
            if (!arrowReaderIterator.hasNext()) {
                return false;
            }
            currentIterator = toArrowRows(arrowReaderIterator.next(), columnsInOrder);
            arrowReaderIterator.next();
        }

        return currentIterator.hasNext();
    }

    @Override
    public InternalRow next() {
        return currentIterator.next();
    }

    private Iterator<InternalRow> toArrowRows(VectorSchemaRoot root, List<String> namesInOrder) {
        List<FieldVector> vectors = namesInOrder.stream().map(name -> root.getVector(name)).collect(Collectors.toList());
        ColumnVector[] columns = vectors.stream().map(vector -> new ArrowSchemaConverter(vector))
                .collect(Collectors.toList()).toArray(new ColumnVector[0]);

        ColumnarBatch batch = new ColumnarBatch(columns);
        batch.setNumRows(root.getRowCount());
        return batch.rowIterator();
    }
}

class ArrowReaderIterator implements Iterator<VectorSchemaRoot> {

    boolean closed = false;
    VectorSchemaRoot current = null;
    ArrowReader reader;
    private static final Logger log = LoggerFactory.getLogger(AvroBinaryIterator.class);

    public ArrowReaderIterator(ArrowReader reader) {
        this.reader = reader;
    }

    @Override
    public boolean hasNext() {
        if (current != null) {
            return true;
        }

        try {
            boolean res = reader.loadNextBatch();
            if (res) {
                current = reader.getVectorSchemaRoot();
            } else {
                ensureClosed();
            }
            return res;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public VectorSchemaRoot next() {
        VectorSchemaRoot res = current;
        current = null;
        return res;
    }

    private void ensureClosed() throws IOException {
        if (!closed) {
            reader.close();
            closed = true;
        }
    }
}
