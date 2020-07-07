package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1alpha2.Storage;
import com.google.protobuf.Descriptors;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static com.google.cloud.spark.bigquery.ProtobufUtils.toDescriptor;
import static com.google.cloud.spark.bigquery.ProtobufUtils.toProtoSchema;
import static com.google.cloud.spark.bigquery.SchemaConverters.toBigQuerySchema;

public class BigQueryDataSourceWriter implements DataSourceWriter {

    final Logger logger = LoggerFactory.getLogger(BigQueryDataSourceWriter.class);

    private BigQuery bigquery;
    private final BigQueryWriteClient client;

    private String datasetName;
    private String tableName;
    private StructType sparkSchema;
    private Schema bigQuerySchema;

    private TableInfo tableInfo;
    private String tableId;
    private String tableIdForReading;

    private boolean ignoreInputs = false;
    private String writeUUID;

    public BigQueryDataSourceWriter(SaveMode saveMode, StructType sparkSchema, String writeUUID,
                                    String tableName, String datasetName) {
        logger.debug("BigQueryDataSourceWriter( {}, {}, {}, {}, {} )", saveMode, sparkSchema, writeUUID, tableName, datasetName);
        // TODO: Guice.createInjector()
        this.writeUUID = writeUUID;
        this.datasetName = datasetName;
        this.tableName = tableName;
        this.sparkSchema = sparkSchema;
        this.bigQuerySchema = toBigQuerySchema(sparkSchema);
        this.tableInfo =
                TableInfo.newBuilder(
                        TableId.of(datasetName, tableName),
                        StandardTableDefinition.of(
                                bigQuerySchema))
                        .build();
        this.tableId =
                String.format(
                        "projects/%s/datasets/%s/tables/%s",
                        ServiceOptions.getDefaultProjectId(), datasetName, tableName);
        this.tableIdForReading =
                String.format(
                        "%s:%s.%s",
                        ServiceOptions.getDefaultProjectId(), datasetName, tableName
                );

        this.bigquery = BigQueryOptions.getDefaultInstance().getService();
        try {
            this.client = BigQueryWriteClient.create();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize BigQueryWriteClient.", e);
        }

        checkIfTableExists(saveMode);
    }

    private void checkIfTableExists(SaveMode saveMode) {
        if(tableExists()) {
            switch (saveMode) {
                case Append:
                    break; // ? TODO: truncation table stuff...
                case Overwrite:
                    try{
                        assert(getDataset().get(tableName).delete());
                    }
                    catch (AssertionError e) {
                        throw new RuntimeException("Failed to delete existing table in BigQuery.", e);
                    }
                    createBigQueryTable();
                case Ignore:
                    ignoreInputs = true;
                case ErrorIfExists:
                    throw new RuntimeException("Table already exists in BigQuery."); // TODO: should this be a RuntimeException?
            }
        }
        else {
            createBigQueryTable();
        }
    }

    private void createBigQueryTable() {
        bigquery.create(tableInfo);
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new BigQueryDataWriterFactory(writeUUID, tableId, tableIdForReading, bigQuerySchema, sparkSchema, ignoreInputs);
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
                            .setParent(tableId);
            for(WriterCommitMessage message : messages) {
                batchCommitWriteStreamsRequest.addWriteStreams(((BigQueryWriterCommitMessage)message).getWriteStreamName());
            }
            Storage.BatchCommitWriteStreamsResponse batchCommitWriteStreamsResponse = client.batchCommitWriteStreams(
                    batchCommitWriteStreamsRequest.build());

            logger.info("BigQuery DataSource writer has committed at time: {}.", batchCommitWriteStreamsResponse.getCommitTime());
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        logger.warn("BigQuery Data Source writer {} aborted.", writeUUID);
        client.close();
    }

    private boolean tableExists() {
        return getDataset().get(tableName) != null;
    }

    private Dataset getDataset() {
        return bigquery.getDataset(datasetName);
    }
}