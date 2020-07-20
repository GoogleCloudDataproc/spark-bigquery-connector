package com.google.cloud.spark.bigquery.v2;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1alpha2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class BigQueryDataWriterHelper {

  final Logger logger = LoggerFactory.getLogger(BigQueryDataWriterHelper.class);
  final long APPEND_REQUEST_SIZE = 1000L * 1000L; // 1MB limit for each append

  private final BigQueryWriteClient writeClient;
  private final String tablePath;
  private final ProtoBufProto.ProtoSchema protoSchema;

  private Stream.WriteStream writeStream;
  private StreamWriter streamWriter;
  private ProtoBufProto.ProtoRows.Builder protoRows;

  private long appendRows = 0; // number of rows waiting for the next append request
  private long appendBytes = 0; // number of bytes waiting for the next append request

  private long writeStreamBytes = 0; // total bytes of the current write-stream
  private long writeStreamRows = 0; // total offset / rows of the current write-stream

  protected BigQueryDataWriterHelper(
      BigQueryWriteClientFactory writeClientFactory,
      String tablePath,
      StructType sparkSchema,
      ProtoBufProto.ProtoSchema protoSchema) {
    this.writeClient = writeClientFactory.createBigQueryWriteClient();
    this.tablePath = tablePath;
    this.protoSchema = protoSchema;

    createWriteStreamAndStreamWriter();
    this.protoRows = ProtoBufProto.ProtoRows.newBuilder();
  }

  private void createWriteStreamAndStreamWriter() {
    this.writeStream =
        writeClient.createWriteStream(
            Storage.CreateWriteStreamRequest.newBuilder()
                .setParent(tablePath)
                .setWriteStream(
                    Stream.WriteStream.newBuilder()
                        .setType(Stream.WriteStream.Type.PENDING)
                        .build())
                .build());

    try {
      this.streamWriter = StreamWriter.newBuilder(writeStream.getName()).build();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Could not build stream-writer.", e);
    }
  }

  protected void addRow(ByteString message) throws IOException {
    int messageSize = message.size();

    if (appendBytes + messageSize > APPEND_REQUEST_SIZE) {
      appendRequest();
      appendRows = 0;
      appendBytes = 0;
    }
    /*
           if(writeStreamBytes + messageSize > MAX_STREAM_SIZE) {
               throw new IOException("Data writer exceeded maximum write-stream size"); // suppress error.
           }
    */

    protoRows.addSerializedRows(message);
    appendBytes += messageSize;
    appendRows++;
  }

  private void appendRequest() throws IOException, IllegalStateException {
    Storage.AppendRowsRequest.Builder requestBuilder =
        Storage.AppendRowsRequest.newBuilder().setOffset(Int64Value.of(writeStreamRows));

    Storage.AppendRowsRequest.ProtoData.Builder dataBuilder =
        Storage.AppendRowsRequest.ProtoData.newBuilder();
    dataBuilder.setWriterSchema(protoSchema);
    dataBuilder.setRows(protoRows.build());

    requestBuilder.setProtoRows(dataBuilder.build()).setWriteStream(writeStream.getName());

    ApiFuture<Storage.AppendRowsResponse> response = streamWriter.append(requestBuilder.build());

    try {
      if (this.writeStreamRows != response.get().getOffset()) {
        throw new IllegalStateException("Append request offset did not match expected offset.");
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Could not get offset for append request.", e);
    }

    clearProtoRows();
    this.writeStreamRows += appendRows; // add the # of rows appended to writeStreamRows
    this.writeStreamBytes += appendBytes;
  }

  protected void finalizeStream() throws IOException {
    if (this.appendRows != 0 || this.appendBytes != 0) {
      appendRequest();
    }

    Storage.FinalizeWriteStreamResponse finalizeResponse =
        writeClient.finalizeWriteStream(
            Storage.FinalizeWriteStreamRequest.newBuilder().setName(writeStream.getName()).build());

    if (finalizeResponse.getRowCount() != writeStreamRows) {
      throw new IOException("Finalize response had an unexpected row count.");
    }

    writeClient.shutdown();

    logger.debug(
        "Write-stream {} finalized with row-count {}",
        writeStream.getName(),
        finalizeResponse.getRowCount());
  }

  private void clearProtoRows() {
    this.protoRows.clear();
  }

  protected String getWriteStreamName() {
    return writeStream.getName();
  }

  protected long getDataWriterRows() {
    return writeStreamRows;
  }

  protected void abort() {
    if (streamWriter != null) {
      streamWriter.close();
    }
    if (writeClient != null && !writeClient.isShutdown()) {
      writeClient.shutdown();
    }
  }
}
