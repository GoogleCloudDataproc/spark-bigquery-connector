package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;

public class BigQueryIndirectDataWriterFactory implements DataWriterFactory<InternalRow> {
    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
        return new BigQueryIndirectDataWriter();
    }
}
