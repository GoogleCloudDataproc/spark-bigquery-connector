package com.google.cloud.spark.bigquery.v2;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1alpha2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Int64Value;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.google.cloud.spark.bigquery.ProtobufUtils.buildSingleRowMessage;
import static com.google.cloud.spark.bigquery.ProtobufUtils.toDescriptor;
import static junit.framework.Assert.assertEquals;

public class BigQueryDataWriter implements DataWriter<InternalRow> {

    final Logger logger = LoggerFactory.getLogger(BigQueryDataWriter.class);
    private final long APPEND_REQUEST_SIZE = 1000L * 1000L; // 1MB limit for append requests

    private final int partitionId;
    private final long taskId;
    private final long epochId;
    private final String tablePath;
    private final StructType sparkSchema;
    private final Descriptors.Descriptor schemaDescriptor;
    private final ProtoBufProto.ProtoSchema protoSchema;
    private final boolean ignoreInputs;

    private BigQueryDataWriterHelper writerHelper;

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

        this.writerHelper = new BigQueryDataWriterHelper(writeClientFactory,
                tablePath, sparkSchema, protoSchema);
    }

    @Override
    public void write(InternalRow record) throws IOException {
        if(ignoreInputs) return;

        ByteString message = buildSingleRowMessage(sparkSchema, schemaDescriptor, record).toByteString();
        writerHelper.addRow(message);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        logger.debug("Data Writer {} commit()", partitionId);

        Long finalizedRowCount = null;
        List<String> writeStreamNames = null;

        if (!ignoreInputs) {
            writerHelper.closeHelperAndFinalizeStream();

            finalizedRowCount = writerHelper.getDataWriterRows();
            writeStreamNames = writerHelper.getWriteStreamNames();

            logger.debug("Data Writer {}'s write-stream has finalized with row count: {}", partitionId, finalizedRowCount);
        }

        return new BigQueryWriterCommitMessage(writeStreamNames, partitionId, taskId, epochId, tablePath, finalizedRowCount);
    }

    @Override
    public void abort() throws IOException {
        logger.debug("Data Writer {} abort()", partitionId);
        // TODO
    }
}