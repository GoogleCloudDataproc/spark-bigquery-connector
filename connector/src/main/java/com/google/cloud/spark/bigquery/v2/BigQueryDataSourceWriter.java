package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClientForWriting;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.connector.common.WriteSessionConfig;
import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1alpha2.Storage;
import com.google.common.base.Preconditions;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class BigQueryDataSourceWriter implements DataSourceWriter {

    final Logger logger = LoggerFactory.getLogger(BigQueryDataSourceWriter.class);

    private BigQueryClientForWriting bigQueryClient;
    private final BigQueryWriteClient writeClient;
    private final BigQueryWriteClientFactory writeClientFactory;
    private final WriteSessionConfig writeSessionConfig;
    private final TableInfo tableInfo;
    private final TableId tableId;
    private final String writeUUID;
    private final SaveMode saveMode;

    private boolean ignoreInputs = false;

    public BigQueryDataSourceWriter(TableInfo tableInfo, BigQueryClientForWriting bigQueryClient,
                                    BigQueryWriteClientFactory bigQueryWriteClientFactory,
                                    WriteSessionConfig writeSessionConfig) {
        this.tableInfo = tableInfo;
        this.tableId = writeSessionConfig.getTableId();
        this.bigQueryClient = bigQueryClient;
        this.writeClientFactory = bigQueryWriteClientFactory;
        this.writeSessionConfig = writeSessionConfig;
        this.writeUUID = writeSessionConfig.getWriteUUID();
        this.saveMode = writeSessionConfig.getSaveMode();
        this.writeClient = writeClientFactory.createBigQueryWriteClient();
        checkIfTableExists(saveMode);
    }

    private void checkIfTableExists(SaveMode saveMode) {
        if(bigQueryClient.tableExists(tableId)) {
            switch (saveMode) {
                case Append:
                    break; // TODO: truncation table stuff...
                case Overwrite:
                    Preconditions.checkArgument(bigQueryClient.deleteTable(tableId),
                            new IOException("Could not delete an existing table in BigQuery, in order to overwrite."));
                    createBigQueryTable();
                    break;
                case Ignore:
                    ignoreInputs = true;
                    break;
                case ErrorIfExists:
                    throw new RuntimeException("Table already exists in BigQuery."); // TODO: should this be a RuntimeException?
            }
        }
        else {
            createBigQueryTable();
        }
    }

    private void createBigQueryTable() {
        bigQueryClient.createTable(tableInfo);
        logger.debug("Created table at {}", tableInfo);
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new BigQueryDataWriterFactory(writeClientFactory, writeSessionConfig, ignoreInputs);
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {

    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        logger.info("BigQuery DataSource writer {} committed with messages:\n{}", writeUUID, Arrays.toString(messages));

        if (!ignoreInputs) {
            Storage.BatchCommitWriteStreamsRequest.Builder batchCommitWriteStreamsRequest =
                    Storage.BatchCommitWriteStreamsRequest.newBuilder()
                            .setParent(tableId.toString());
            for(WriterCommitMessage message : messages) {
                batchCommitWriteStreamsRequest.addWriteStreams(((BigQueryWriterCommitMessage)message)
                        .getWriteStreamName().get());
            }
            Storage.BatchCommitWriteStreamsResponse batchCommitWriteStreamsResponse = writeClient.batchCommitWriteStreams(
                    batchCommitWriteStreamsRequest.build());

            logger.info("BigQuery DataSource writer has committed at time: {}.", batchCommitWriteStreamsResponse.getCommitTime());
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        logger.warn("BigQuery Data Source writer {} aborted.", writeUUID);
        writeClient.close();
    }
}