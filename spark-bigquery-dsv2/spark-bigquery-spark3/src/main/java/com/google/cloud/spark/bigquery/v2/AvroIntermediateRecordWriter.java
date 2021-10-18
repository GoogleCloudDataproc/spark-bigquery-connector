package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.common.GenericAvroIntermediateRecordWriter;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroIntermediateRecordWriter extends GenericAvroIntermediateRecordWriter
        implements IntermediateRecordWriter {

    AvroIntermediateRecordWriter(Schema schema, OutputStream outputStream) throws IOException {
        super(schema, outputStream);
    }

    @Override
    public void write(GenericRecord record) throws IOException {
        getDataFileWriter().append(record);
    }

    @Override
    public void close() throws IOException {
        try {
            getDataFileWriter().flush();
        } finally {
            getDataFileWriter().close();
        }
    }
}