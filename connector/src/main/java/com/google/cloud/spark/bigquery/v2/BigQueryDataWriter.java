package com.google.cloud.spark.bigquery.v2;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
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

import static com.google.cloud.spark.bigquery.ProtobufUtils.buildSingleRowMessage;
import static com.google.cloud.spark.bigquery.ProtobufUtils.toDescriptor;
import static junit.framework.Assert.assertEquals;

public class BigQueryDataWriter implements DataWriter<InternalRow> {

    final Logger logger = LoggerFactory.getLogger(BigQueryDataWriter.class);
    private final int APPEND_REQUEST_SIZE = 1000;

    private final int partitionId;
    private final long taskId;
    private final long epochId;
    private final String tablePath;
    private final StructType sparkSchema;
    private final Descriptors.Descriptor schemaDescriptor;
    private final ProtoBufProto.ProtoSchema protoSchema;
    private final boolean ignoreInputs;

    private BigQueryWriteClient writeClient;
    private Stream.WriteStream writeStream;
    private ProtoBufProto.ProtoRows.Builder protoRows;
    private int rowCounter;
    private int offset;

    public BigQueryDataWriter(int partitionId, long taskId, long epochId, BigQueryWriteClientFactory writeClientFactory,
                              String tablePath, StructType sparkSchema, ProtoBufProto.ProtoSchema protoSchema,
                              boolean ignoreInputs) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.tablePath = tablePath;
        this.sparkSchema = sparkSchema;
        try {
            this.schemaDescriptor = toDescriptor(sparkSchema);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException("Could not convert spark-schema to descriptor object.", e);
        }
        this.protoSchema = protoSchema;
        this.ignoreInputs = ignoreInputs;

        if (ignoreInputs) return;

        this.writeClient = writeClientFactory.createBigQueryWriteClient();
        Stream.WriteStream aWriteStream = Stream.WriteStream.newBuilder()
                .setType(Stream.WriteStream.Type.PENDING).build();
        this.writeStream =
                writeClient.createWriteStream(
                        Storage.CreateWriteStreamRequest.newBuilder()
                                .setParent(tablePath)
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
            appendRequest();
            protoRows = ProtoBufProto.ProtoRows.newBuilder();
            this.offset += APPEND_REQUEST_SIZE;
        }

        protoRows.addSerializedRows(buildSingleRowMessage(sparkSchema, schemaDescriptor, record)
                .toByteString());
    }

    public void appendRequest() throws IOException {
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
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        logger.debug("Data Writer {} commit()", partitionId);

        Long rowCount = null;
        String writeStreamName = null;

        if (!ignoreInputs) {
            // Append all leftover since the last append:
            if(protoRows.getSerializedRowsCount() > 0) {
                appendRequest();
            }

            Storage.FinalizeWriteStreamResponse finalizeResponse =
                    writeClient.finalizeWriteStream(
                            Storage.FinalizeWriteStreamRequest.newBuilder()
                                    .setName(writeStream.getName()).build());

            writeStreamName = writeStream.getName();
            rowCount = finalizeResponse.getRowCount();

            writeClient.shutdown();

            logger.debug("Data Writer {}'s write-stream has finalized with row count: {}", partitionId, rowCount);
        }

        return new BigQueryWriterCommitMessage(writeStreamName, partitionId, taskId, epochId, tablePath, rowCount);
    }

    @Override
    public void abort() throws IOException {
        logger.debug("Data Writer {} abort()", partitionId);
        // TODO
    }
}