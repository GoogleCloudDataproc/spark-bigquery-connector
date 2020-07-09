package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.connector.common.WriteSessionConfig;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDataWriterFactory implements DataWriterFactory<InternalRow> {

    final Logger logger = LoggerFactory.getLogger(BigQueryDataWriterFactory.class);

    private final BigQueryWriteClientFactory writeClientFactory;
    private final WriteSessionConfig writeSessionConfig;
    private final boolean ignoreInputs;

    public BigQueryDataWriterFactory(BigQueryWriteClientFactory writeClientFactory,
                                     WriteSessionConfig writeSessionConfig,
                                     boolean ignoreInputs) {
        this.writeClientFactory = writeClientFactory;
        this.writeSessionConfig = writeSessionConfig;
        this.ignoreInputs = ignoreInputs;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
        return new BigQueryDataWriter(partitionId, taskId, epochId, writeClientFactory,
                writeSessionConfig, ignoreInputs);
    }
}