package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.Schema;
import com.google.protobuf.ByteString;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;

public class AvroBinaryIterator implements Iterator<InternalRow> {

    GenericDatumReader reader;
    List<String> columnsInOrder;
    BinaryDecoder in;
    Schema bqSchema;
    private static final Logger log = LoggerFactory.getLogger(AvroBinaryIterator.class);

    /**
     * An iterator for scanning over rows serialized in Avro format
     * @param bqSchema Schema of underlying BigQuery source
     * @param columnsInOrder Sequence of columns in the schema
     * @param schema Schema in avro format
     * @param rowsInBytes Rows serialized in binary format for Avro
     */
    public AvroBinaryIterator(Schema bqSchema,
                              List<String> columnsInOrder,
                              org.apache.avro.Schema schema,
                              ByteString rowsInBytes) {
        reader = new GenericDatumReader<GenericRecord>(schema);
        this.bqSchema = bqSchema;
        this.columnsInOrder = columnsInOrder;
        in = new DecoderFactory().binaryDecoder(rowsInBytes.toByteArray(), null);
    }

    @Override
    public boolean hasNext() {
        try {
            return !in.isEnd();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public InternalRow next() {
        try {
            return SchemaConverters.createRowConverter(bqSchema,
                    columnsInOrder, (GenericRecord) reader.read(null, in));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
