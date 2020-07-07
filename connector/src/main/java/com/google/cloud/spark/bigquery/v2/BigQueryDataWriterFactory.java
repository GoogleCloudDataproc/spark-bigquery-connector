package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.protobuf.Descriptors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BigQueryDataWriterFactory implements DataWriterFactory<InternalRow> {

    final Logger logger = LoggerFactory.getLogger(BigQueryDataWriterFactory.class);

    private final BigQueryWriteClient client;
    private final Schema bigQuerySchema;
    private final StructType sparkSchema;
    private final Descriptors.Descriptor schemaDescriptor;
    private final ProtoBufProto.ProtoSchema protoSchema;
    private final String writeUUID;
    private final String tableId;
    private final String tableIdForReading;
    private final boolean ignoreInputs;

    public BigQueryDataWriterFactory(String writeUUID, String tableId, String tableIdForReading,
                                     Schema bigQuerySchema, StructType sparkSchema,
                                     Descriptors.Descriptor schemaDescriptor, ProtoBufProto.ProtoSchema protoSchema,
                                     boolean ignoreInputs, BigQueryWriteClient client) {
        logger.debug("BigQueryWriterFactory( {}, {}, {}, {}, {}, {}, {}, {}, {})", writeUUID, tableId, tableIdForReading,
                bigQuerySchema, sparkSchema, schemaDescriptor, protoSchema, ignoreInputs, client);
        this.writeUUID = writeUUID;
        this.tableId = tableId;
        this.tableIdForReading = tableIdForReading;
        this.bigQuerySchema = bigQuerySchema;
        this.sparkSchema = sparkSchema;
        this.schemaDescriptor = schemaDescriptor;
        this.protoSchema = protoSchema;
        this.ignoreInputs = ignoreInputs;
        this.client = client;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
        return new BigQueryDataWriter(partitionId, taskId, epochId, client, writeUUID, tableId, tableIdForReading,
                bigQuerySchema, sparkSchema, schemaDescriptor, protoSchema, ignoreInputs);
    }
}