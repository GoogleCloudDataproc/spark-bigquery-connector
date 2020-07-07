package com.google.cloud.spark.bigquery.v2;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1alpha2.*;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Int64Value;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.cloud.spark.bigquery.ProtoBufUtils.createSingleRowMessage;
import static junit.framework.Assert.assertEquals;

public class BigQueryDataWriter implements DataWriter<InternalRow> {

    final Logger logger = LoggerFactory.getLogger(BigQueryDataWriter.class);
    private final int APPEND_REQUEST_SIZE = 1000;

    private final int partitionId;
    private final long taskId;
    private final long epochId;
    private final BigQueryWriteClient client;
    private final String writeUUID;
    private final String tableId;
    private final String tableIdForReading;
    private final Schema bigQuerySchema;
    private final StructType sparkSchema;
    private final Descriptors.Descriptor schemaDescriptor;
    private final ProtoBufProto.ProtoSchema protoSchema;
    private final boolean ignoreInputs;

    private Stream.WriteStream writeStream;
    private ProtoBufProto.ProtoRows.Builder protoRows;
    private int rowCounter;
    private int offset;

    public BigQueryDataWriter(int partitionId, long taskId, long epochId, BigQueryWriteClient client, String writeUUID,
                              String tableId, String tableIdForReading, Schema bigQuerySchema, StructType sparkSchema,
                              Descriptors.Descriptor schemaDescriptor, ProtoBufProto.ProtoSchema protoSchema,
                              boolean ignoreInputs) {
        logger.debug("BigQueryDataWriter( {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} )", partitionId, taskId, epochId,
                client, writeUUID, tableId, tableIdForReading, bigQuerySchema, sparkSchema, schemaDescriptor, protoSchema,
                ignoreInputs);
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.client = client;
        this.writeUUID = writeUUID;
        this.tableId = tableId;
        this.tableIdForReading = tableIdForReading;
        this.bigQuerySchema = bigQuerySchema;
        this.sparkSchema = sparkSchema;
        this.schemaDescriptor = schemaDescriptor;
        this.protoSchema = protoSchema;
        this.ignoreInputs = ignoreInputs;

        Stream.WriteStream aWriteStream = Stream.WriteStream.newBuilder()
                .setType(Stream.WriteStream.Type.PENDING).build();
        this.writeStream =
                client.createWriteStream(
                        Storage.CreateWriteStreamRequest.newBuilder()
                                .setParent(tableId)
                                .setWriteStream(aWriteStream)
                                .build());
        this.protoRows = ProtoBufProto.ProtoRows.newBuilder();
        this.rowCounter = 0;
        this.offset = 0;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        logger.debug("DataWriter {} write( {} )", partitionId, record);
        if(ignoreInputs) return;
        if(rowCounter >= APPEND_REQUEST_SIZE) {
            Storage.AppendRowsRequest.Builder requestBuilder = Storage.AppendRowsRequest.newBuilder()
                    .setOffset(Int64Value.of(offset));
            Storage.AppendRowsRequest.ProtoData.Builder dataBuilder =
                    Storage.AppendRowsRequest.ProtoData.newBuilder();
            dataBuilder.setWriterSchema(protoSchema);

            dataBuilder.setRows(protoRows.build());
            requestBuilder
                    .setProtoRows(dataBuilder.build())
                    .setWriteStream(writeStream.getName());

            // Append call
            try (StreamWriter streamWriter =
                         StreamWriter.newBuilder(writeStream.getName()).build()) {
                ApiFuture<Storage.AppendRowsResponse> response =
                        streamWriter.append(requestBuilder.build());
                try {
                    assertEquals(this.offset, response.get().getOffset());
                } catch (Exception e) {
                    logger.error("Append request had an offset that was not expected.", e);
                    abort();
                }
            } catch (InterruptedException e) {
                logger.error("Stream writer had an interrupted build.", e);
                abort();
            }

            this.offset += APPEND_REQUEST_SIZE;
        }
        else {
            protoRows.addSerializedRows(createSingleRowMessage(sparkSchema, schemaDescriptor, record).toByteString());
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        logger.debug("Data Writer {} commit()", partitionId);
        Storage.FinalizeWriteStreamResponse finalizeResponse =
                client.finalizeWriteStream(
                        Storage.FinalizeWriteStreamRequest.newBuilder()
                                .setName(writeStream.getName()).build());

        long rowCount = finalizeResponse.getRowCount();

        return new BigQueryWriterCommitMessage(writeStream.getName(), partitionId, taskId, epochId, tableIdForReading, rowCount);
    }

    @Override
    public void abort() throws IOException {
        // TODO
    }
}