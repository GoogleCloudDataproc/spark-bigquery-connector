package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

// TODO: is this snappy implementation better tha org.xerial.snappy.Snappy
// import io.netty.handler.codec.compression.Snappy;

public class DecompressReadRowsResponse {
  private static long linesToLog = 10; // reduce spam only return this number of lines
  private static final Logger log = LoggerFactory.getLogger(DecompressReadRowsResponse.class);

  /** Returns a new allocator limited to maxAllocation bytes */
  public static InputStream decompressArrowRecordBatch(
      ReadRowsResponse response, boolean lz4Compression) {

    long statedUncompressedByteSize =
        response.getUnknownFields().getField(9).getVarintList().get(0);

    if (statedUncompressedByteSize <= 0) {
      if (linesToLog > 0) {
        log.info(
            "AQIU: DecompressReadRowsResponse found uncompressedByteSize is 0 or less. it is {}",
            statedUncompressedByteSize);
        linesToLog--;
      }

      // no compession here, return directly
      return response.getArrowRecordBatch().getSerializedRecordBatch().newInput();
    }

    try {
      if (lz4Compression == true) {
        // TODO:
      } else {
        // Note that if trying to use asReadOnlyByteBuffer() to avoid copying to Byte[] bY using the
        // same backing array as the byte string, if possible, then
        // Snappy.uncompress(response.getArrowRecordBatch().getSerializedRecordBatch().asReadOnlyByteBuffer(),arrowRecordBatchBuffer)
        // does not work because
        // response.getArrowRecordBatch().getSerializedRecordBatch().asReadOnlyByteBuffer() is not a
        // direct buffer, and snappy will give this error:
        // com.google.cloud.spark.bigquery.repackaged.org.xerial.snappy.SnappyError:
        // [NOT_A_DIRECT_BUFFER] input is not a direct buffer
        byte[] decompressedByteArray =
            Snappy.uncompress(
                response.getArrowRecordBatch().getSerializedRecordBatch().toByteArray());

        if (linesToLog > 0) {
          log.info(
              "AQIU: DecompressReadRowsResponse decompressed length  = {} vs uncompessed length {}",
              decompressedByteArray.length,
              statedUncompressedByteSize);
          linesToLog--;
        }

        return new ByteArrayInputStream(decompressedByteArray);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return response.getArrowRecordBatch().getSerializedRecordBatch().newInput();
  }
}
