package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.protobuf.ByteString;
import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

interface ReadRowsResponseToInternalRowIteratorConverter {

    Iterator<InternalRow> convert(Storage.ReadRowsResponse response);

    static Supplier<ReadRowsResponseToInternalRowIteratorConverter> avro(
            final com.google.cloud.bigquery.Schema bqSchema,
            final List<String> columnsInOrder,
            final org.apache.avro.Schema avroSchema) {
        return () ->
                response -> BigQueryUtil.toJavaIterator(
                        new AvroBinaryIterator(
                                bqSchema,
                                BigQueryUtil.toSeq(columnsInOrder),
                                avroSchema,
                                response.getAvroRows().getSerializedBinaryRows()));
    }

    static Supplier<ReadRowsResponseToInternalRowIteratorConverter> arrow(
            final List<String> columnsInOrder,
            final ByteString arrowSchema) {
        return () ->
                response -> BigQueryUtil.toJavaIterator(
                        new ArrowBinaryIterator(
                                BigQueryUtil.toSeq(columnsInOrder),
                                arrowSchema,
                                response.getArrowRecordBatch().getSerializedRecordBatch()));
    }
}
