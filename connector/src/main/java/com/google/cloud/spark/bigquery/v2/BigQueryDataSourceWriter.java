package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1alpha2.Storage;
import com.google.common.base.Preconditions;
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
import static com.google.cloud.spark.bigquery.SchemaConverters.toBigQuerySchema;

public class BigQueryDataSourceWriter implements DataSourceWriter {

    final Logger logger = LoggerFactory.getLogger(BigQueryDataSourceWriter.class);

    private final BigQueryClient bigQueryClient;
    private final BigQueryWriteClientFactory writeClientFactory;
    private final TableId tableId;
    private final String tablePath;
    private final StructType sparkSchema;
    private final Schema bigQuerySchema;
    private final Descriptors.Descriptor schemaDescriptor;
    private final ProtoBufProto.ProtoSchema protoSchema;
    private final SaveMode saveMode;
    private final String writeUUID;

    private final TableInfo table;

    private BigQueryWriteClient writeClient;
    private boolean ignoreInputs = false;

    public BigQueryDataSourceWriter(BigQueryClient bigQueryClient, BigQueryWriteClientFactory bigQueryWriteClientFactory,
                                    TableId tableId, String writeUUID, SaveMode saveMode, StructType sparkSchema) {
        this.bigQueryClient = bigQueryClient;
        this.writeClientFactory = bigQueryWriteClientFactory;
        this.tableId = tableId;
        this.tablePath = String.format(
                "projects/%s/datasets/%s/tables/%s",
                tableId.getProject(), tableId.getDataset(), tableId.getTable());
        this.writeUUID = writeUUID;
        this.saveMode = saveMode;
        this.sparkSchema = sparkSchema;
        this.bigQuerySchema = toBigQuerySchema(sparkSchema);
        try {
            this.schemaDescriptor = toDescriptor(sparkSchema);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException("Could not convert Spark schema to protobuf descriptor.", e);
        }
        this.protoSchema = ProtoSchemaConverter.convert(schemaDescriptor);

        this.table = checkIfTableExists(saveMode);

        if(!ignoreInputs) {
            this.writeClient = writeClientFactory.createBigQueryWriteClient();
        }
    }

    private TableInfo checkIfTableExists(SaveMode saveMode) {
        if(bigQueryClient.tableExists(tableId)) {
            switch (saveMode) {
                case Append:
                    break; // TODO: truncation table stuff...
                case Overwrite:
                    Preconditions.checkArgument(bigQueryClient.deleteTable(tableId),
                            new IOException("Could not delete an existing table in BigQuery, in order to overwrite."));
                    return createBigQueryTable();
                case Ignore:
                    ignoreInputs = true;
                    break;
                case ErrorIfExists:
                    throw new RuntimeException("Table already exists in BigQuery."); // TODO: should this be a RuntimeException?
            }
            return bigQueryClient.getTable(tableId);
        }
        else {
            return createBigQueryTable();
        }
    }

    private TableInfo createBigQueryTable() {
        return bigQueryClient.createTable(tableId, bigQuerySchema);
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new BigQueryDataWriterFactory(writeClientFactory, tablePath, sparkSchema, protoSchema, ignoreInputs);
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
                            .setParent(tablePath);
            for(WriterCommitMessage message : messages) {
                batchCommitWriteStreamsRequest.addWriteStreams(((BigQueryWriterCommitMessage)message)
                        .getWriteStreamName());
            }
            Storage.BatchCommitWriteStreamsResponse batchCommitWriteStreamsResponse = writeClient.batchCommitWriteStreams(
                    batchCommitWriteStreamsRequest.build());

            logger.info("BigQuery DataSource writer has committed at time: {}.", batchCommitWriteStreamsResponse.getCommitTime());

            writeClient.shutdown();
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        logger.warn("BigQuery Data Source writer {} aborted.", writeUUID);
        if (!ignoreInputs) {
            writeClient.shutdown(); // TODO help delete data in intermediary? Or keep in sink until TTL.
        }
    }
}