package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;

public class BigQueryStreamingWriter implements StreamWriter {

    BigQueryStreamingWriter(){

    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {

    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {

    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        //In Development
        // Call BigQueryIndirectWriterFactory
        // Either BigqueryIndirectWriterFactory will be used or new class will be created implementing DataWriterFactory
        // Based on above params will be sent CreateStreamWriter
        // From there BigQueryIndirect will be used

        //return new BigQueryIndirectDataWriterFactory(conf,gcsDirPath,sparkSchema,avrosSchemaJson)
        return null;
    }
}
