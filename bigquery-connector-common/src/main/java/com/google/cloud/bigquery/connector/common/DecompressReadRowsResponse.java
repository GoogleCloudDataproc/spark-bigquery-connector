package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.UnknownFieldSet;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

// import io.netty.handler.codec.compression.Snappy;

public class DecompressReadRowsResponse {
  private static long linesToLog = 100; // reduce spam
  private static final Logger log = LoggerFactory.getLogger(DecompressReadRowsResponse.class);

  /** Returns a new allocator limited to maxAllocation bytes */
  public static InputStream decompressArrowRecordBatch(ReadRowsResponse response) {

    InputStream inputStream = response.getArrowRecordBatch().getSerializedRecordBatch().newInput();

    UnknownFieldSet unknownFieldSet = response.getUnknownFields();
    java.util.Map<Integer, UnknownFieldSet.Field> unknownFieldSetMap = unknownFieldSet.asMap();
    if (linesToLog > 0) {
      log.info(
          "AQIU: ReadRowsResponseToInternalRowIteratorConverter ReadRowsResponse"
              + " UnknownFieldSet.asMap {}",
          unknownFieldSetMap);
      log.info(
          "AQIU: ReadRowsResponseToInternalRowIteratorConverter serializedRecordBatch",
          response.getArrowRecordBatch().getSerializedRecordBatch());
      linesToLog--;
    }

    if (!response.getUnknownFields().hasField(9)) {
      if (linesToLog > 0) {
        log.info("AQIU: missing unknownField 9");
        System.out.println("missing unknown fields 9 ");
        linesToLog--;
      }
      return inputStream;
    }

    if (response.getUnknownFields().getField(9).getFixed64List().size() > 1) {
      if (linesToLog > 0) {
        System.out.println("did not expect more than one result uncompressed_byte_size = 9 ");
        linesToLog--;
      }
      return inputStream;
    }

    long statedUncompressedByteSize =
        response.getUnknownFields().getField(9).getFixed64List().get(0);
    if (statedUncompressedByteSize <= 0) {
      // no compession here
      return inputStream;
    }

    try {
      ByteBuffer arrowRecordBatchBuffer = ByteBuffer.allocate((int) statedUncompressedByteSize);
      // https://cloud.google.com/java/docs/reference/protobuf/latest/com.google.protobuf.ByteString#com_google_protobuf_ByteString_asReadOnlyByteBuffer__
      // Use asReadOnlyByteBuffer() because it tries to avoid a copy Byte[].  The result uses
      // the same backing array as the byte string, if possible.
      // ByteBuffer arrowRecordBatchBuffer = ByteBuffer.allocate(Snappy.uncompressedLength(
      //     response.getArrowRecordBatch().getSerializedRecordBatch().asReadOnlyByteBuffer()));
      long decompressedArrowRecordBatchSize =
          Snappy.uncompress(
              response.getArrowRecordBatch().getSerializedRecordBatch().asReadOnlyByteBuffer(),
              arrowRecordBatchBuffer);
      // assert(decompressedArrowRecordBatchSize > 0);
      assert (decompressedArrowRecordBatchSize == statedUncompressedByteSize);

      // https://stackoverflow.com/questions/4332264/wrapping-a-bytebuffer-with-an-inputstream

      return new ByteArrayInputStream(arrowRecordBatchBuffer.array());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
