package com.google.cloud.spark.bigquery.common;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.io.OutputStream;

public class GenericAvroIntermediateRecordWriter{
    private final OutputStream outputStream;
    private final DatumWriter<GenericRecord> writer;
    private final DataFileWriter<GenericRecord> dataFileWriter;

   public GenericAvroIntermediateRecordWriter(Schema schema, OutputStream outputStream) throws IOException {
        this.outputStream = outputStream;
        this.writer = new GenericDatumWriter<>(schema);
        this.dataFileWriter = new DataFileWriter<>(writer);
        this.dataFileWriter.create(schema, outputStream);
    }

    public DataFileWriter<GenericRecord> getDataFileWriter(){
       return this.dataFileWriter;
    }
}