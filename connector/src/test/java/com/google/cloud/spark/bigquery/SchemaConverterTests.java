package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1alpha2.Stream;
import com.google.cloud.spark.bigquery.v2.YuvalSchemaConverters;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;

import static com.google.common.truth.Truth.*;

import static org.junit.Assert.*;

public class SchemaConverterTests {

    // create .proto for Descriptor
    @Test
    public void testBQSchema1IntegerColumn() throws Exception {
        LogManager.getLogger("com.google.cloud.spark").setLevel(Level.INFO);

        /*
        ProtoBufProto.ProtoSchema expectedSchema = ProtoBufProto.ProtoSchema.parseFrom(

        );
         */

        Schema schema = Schema.of(Field.newBuilder("Numbers", LegacySQLTypeName.INTEGER, (FieldList) null).setMode(Field.Mode.REQUIRED).build());
        ProtoBufProto.ProtoSchema protoSchema = YuvalSchemaConverters.toProtoSchema(schema);

        //assertThat(protoSchema).isEqualTo(expectedSchema);
    }
/*
    @Test
    public void testBigQuery() throws Exception{
        BigQueryClient client = BigQueryWriteClient.create();

        Stream.WriteStream writeStream =
                Stream.WriteStream.newBuilder()
                        .setType(Stream.WriteStream.Type.PENDING).build();

        writeStream =
                client.createWriteStream(
                        CreateWriteStreamRequest.newBuilder()
                                .setParent(tableId)
                                .setWriteStream(writeStream)
                                .build());

// Create AppendRowsRequest
        AppendRowsRequest.Builder requestBuilder = AppendRowsRequest.newBuilder();

        AppendRowsRequest.ProtoData.Builder dataBuilder =
                AppendRowsRequest.ProtoData.newBuilder();
        dataBuilder.setWriterSchema(
                ProtoSchemaConverter.convert(FooType.getDescriptor()));

        ProtoBufProto.ProtoRows.Builder rows = ProtoBufProto.ProtoRows.newBuilder();
        FooType foo = FooType.newBuilder().setFoo(message).build();
        rows.addSerializedRows(foo.toByteString());

        dataBuilder.setRows(rows.build());
        requestBuilder
                .setProtoRows(dataBuilder.build())
                .setWriteStream(streamName);

// Append call
        try (StreamWriter streamWriter =
                     StreamWriter.newBuilder(writeStream.getName()).build()) {
            ApiFuture<AppendRowsResponse> response =
                    streamWriter.append(requestBuilder.build());
            try {
                assertEquals(0, response.get().getOffset());
            } catch (Exception e) {
                // process error.
            }
        }

        FinalizeWriteStreamResponse finalizeResponse =
                client.finalizeWriteStream(
                        FinalizeWriteStreamRequest.newBuilder()
                                .setName(writeStream.getName()).build());

        BatchCommitWriteStreamsResponse batchCommitWriteStreamsResponse =
                client.batchCommitWriteStreams(
                        BatchCommitWriteStreamsRequest.newBuilder()
                                .setParent(tableId2)
                                .addWriteStreams(writeStream.getName())
                                .build());
    }
    */

}