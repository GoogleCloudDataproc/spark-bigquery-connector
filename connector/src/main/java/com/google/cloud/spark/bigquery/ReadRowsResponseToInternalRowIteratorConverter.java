package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.protobuf.ByteString;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

interface ReadRowsResponseToInternalRowIteratorConverter {

    Iterator<InternalRow> convert(Storage.ReadRowsResponse response);

    static ReadRowsResponseToInternalRowIteratorConverter avro(
            final com.google.cloud.bigquery.Schema bqSchema,
            final List<String> columnsInOrder,
            final String rawAvroSchema) {
        return new Avro(bqSchema, columnsInOrder, rawAvroSchema);
    }

    static ReadRowsResponseToInternalRowIteratorConverter arrow(
            final List<String> columnsInOrder,
            final ByteString arrowSchema) {
        return new Arrow(columnsInOrder, arrowSchema);
    }

    class Avro implements ReadRowsResponseToInternalRowIteratorConverter, Serializable {

        private final com.google.cloud.bigquery.Schema bqSchema;
        private final List<String> columnsInOrder;
        private final String rawAvroSchema;

        public Avro(Schema bqSchema, List<String> columnsInOrder, String rawAvroSchema) {
            this.bqSchema = bqSchema;
            this.columnsInOrder = columnsInOrder;
            this.rawAvroSchema = rawAvroSchema;
        }

        @Override
        public Iterator<InternalRow> convert(Storage.ReadRowsResponse response) {
            return BigQueryUtil.toJavaIterator(
                    new AvroBinaryIterator(
                            bqSchema,
                            BigQueryUtil.toSeq(columnsInOrder),
                            new org.apache.avro.Schema.Parser().parse(rawAvroSchema),
                            response.getAvroRows().getSerializedBinaryRows()));
        }
    }

    class Arrow implements ReadRowsResponseToInternalRowIteratorConverter, Serializable {

        private final List<String> columnsInOrder;
        private final ByteString arrowSchema;

        public Arrow(List<String> columnsInOrder, ByteString arrowSchema) {
            this.columnsInOrder = columnsInOrder;
            this.arrowSchema = arrowSchema;
        }

        @Override
        public Iterator<InternalRow> convert(Storage.ReadRowsResponse response) {
            return BigQueryUtil.toJavaIterator(
                    new ArrowBinaryIterator(
                            BigQueryUtil.toSeq(columnsInOrder),
                            arrowSchema,
                            response.getArrowRecordBatch().getSerializedRecordBatch()));
        }
    }

//    class AvroIterator implements Iterator<InternalRow>, Serializable {
//
//        private GenericDatumReader reader;
//
//        public AvroIterator(
//                Schema bqSchema,
//                List<String> columnsInOrder,
//                org.apache.avro.Schema avroSchema,
//                AvroRows avroRows) {
//            this.bqSchema = bqSchema;
//            this.columnsInOrder = columnsInOrder;
//            this.rawAvroSchema = rawAvroSchema;
//        }
//
//        private lazy val
//        converter =SchemaConverters.createRowConverter(bqSchema,columnsInOrder)_
//        val reader = new GenericDatumReader[GenericRecord](schema)
//        val in:BinaryDecoder =new
//
//        DecoderFactory().
//
//        binaryDecoder(rowsInBytes.toByteArray, null)
//
//        override def
//        hasNext:Boolean =
//
//        override def
//
//        next():InternalRow =
//
//
//        @Override
//        public boolean hasNext() {
//            return !in.isEnd();
//        }
//
//        @Override
//        public InternalRow next() {
//            return converter(reader.read(null, in));
//        }
//    }
}
