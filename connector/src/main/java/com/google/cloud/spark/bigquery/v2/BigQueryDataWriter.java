package com.google.cloud.spark.bigquery.v2;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.connector.common.WriteSessionConfig;
import com.google.cloud.bigquery.storage.v1alpha2.*;
import com.google.protobuf.Int64Value;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static com.google.cloud.spark.bigquery.ProtobufUtils.buildSingleRowMessage;
import static junit.framework.Assert.assertEquals;

public class BigQueryDataWriter implements DataWriter<InternalRow> {

    final Logger logger = LoggerFactory.getLogger(BigQueryDataWriter.class);
    private final int APPEND_REQUEST_SIZE = 1000;

    private final int partitionId;
    private final long taskId;
    private final long epochId;
    private final BigQueryWriteClient writeClient;
    private final TableId tableId;
    private final StructType sparkSchema;
    private final ProtoBufProto.ProtoSchema protoSchema;
    private final boolean ignoreInputs;

    private Stream.WriteStream writeStream;
    private ProtoBufProto.ProtoRows.Builder protoRows;
    private int rowCounter;
    private int offset;

    public BigQueryDataWriter(int partitionId, long taskId, long epochId, BigQueryWriteClientFactory writeClientFactory,
                              WriteSessionConfig writeSessionConfig, boolean ignoreInputs) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.writeClient = writeClientFactory.createBigQueryWriteClient();
        this.tableId = writeSessionConfig.getTableId();
        this.sparkSchema = writeSessionConfig.getSparkSchema();
        this.protoSchema = writeSessionConfig.getProtoSchema();
        this.ignoreInputs = ignoreInputs;

        if (ignoreInputs) return;

        Stream.WriteStream aWriteStream = Stream.WriteStream.newBuilder()
                .setType(Stream.WriteStream.Type.PENDING).build();
        this.writeStream =
                writeClient.createWriteStream(
                        Storage.CreateWriteStreamRequest.newBuilder()
                                .setParent(tableId.toString())
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

        protoRows.addSerializedRows(buildSingleRowMessage(sparkSchema, protoSchema.getDescriptorForType(), record)
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

        Optional<Long> rowCount = Optional.empty();
        Optional<String> writeStreamName = Optional.empty();

        if (!ignoreInputs) {
            // Append all leftover since the last append:
            if(protoRows.getSerializedRowsCount() > 0) {
                appendRequest();
            }

            Storage.FinalizeWriteStreamResponse finalizeResponse =
                    writeClient.finalizeWriteStream(
                            Storage.FinalizeWriteStreamRequest.newBuilder()
                                    .setName(writeStream.getName()).build());

            writeStreamName = Optional.of(writeStream.getName());
            rowCount = Optional.of(finalizeResponse.getRowCount());

            logger.debug("Data Writer {}'s write-stream has finalized with row count: {}", partitionId, rowCount.get());
        }

        return new BigQueryWriterCommitMessage(writeStreamName, partitionId, taskId, epochId, tableId, rowCount);
    }

    @Override
    public void abort() throws IOException {
        logger.debug("Data Writer {} abort()", partitionId);
        // TODO
    }
}