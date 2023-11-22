package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.UnknownFieldSet;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.xerial.snappy.Snappy;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

public class DecompressReadRowsResponse {
  // private static long linesToLog = 100; // reduce spam - only return this number of lines
  // private static final Logger log = LoggerFactory.getLogger(DecompressReadRowsResponse.class);

  /** Returns a new allocator limited to maxAllocation bytes */
  public static InputStream decompressArrowRecordBatch(
      ReadRowsResponse response, boolean lz4Compression) {

    UnknownFieldSet.Field newField = response.getUnknownFields().getField(9);
    if (newField.getVarintList().size() == 0) {
      // no compression here, return directly
      return response.getArrowRecordBatch().getSerializedRecordBatch().newInput();
    }

    long statedUncompressedByteSize = newField.getVarintList().get(0);
    if (statedUncompressedByteSize <= 0) {
      // no compession here, return directly
      return response.getArrowRecordBatch().getSerializedRecordBatch().newInput();
    }

    try {
      byte[] compressed = response.getArrowRecordBatch().getSerializedRecordBatch().toByteArray();
      if (lz4Compression == true) {
        // TODO: https://github.com/lz4/lz4-java
        LZ4Factory factory = LZ4Factory.fastestInstance();

        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        byte[] decompressed = new byte[(int) statedUncompressedByteSize];
        decompressor.decompress(compressed, 0, decompressed, 0, (int) statedUncompressedByteSize);

        // if (linesToLog > 0) {
        //   log.info(
        //       "AQIU: DecompressReadRowsResponse lz4 compressed decompressed length = {} vs"
        //           + " uncompressed length {}",
        //       decompressed.length,
        //       statedUncompressedByteSize);
        //   linesToLog--;
        // }

        return new ByteArrayInputStream(decompressed);

      } else {
        // Note that if trying to use asReadOnlyByteBuffer() to avoid copying to Byte[] bY using the
        // same backing array as the byte string, if possible, then
        // Snappy.uncompress(response.getArrowRecordBatch().getSerializedRecordBatch().asReadOnlyByteBuffer(),arrowRecordBatchBuffer)
        // does not work because
        // response.getArrowRecordBatch().getSerializedRecordBatch().asReadOnlyByteBuffer() is not a
        // direct buffer, and snappy will give this error:
        // com.google.cloud.spark.bigquery.repackaged.org.xerial.snappy.SnappyError:
        // [NOT_A_DIRECT_BUFFER] input is not a direct buffer
        byte[] decompressed = Snappy.uncompress(compressed);

        // if (linesToLog > 0) {
        //   log.info(
        //       "AQIU: DecompressReadRowsResponse snappy decompressed length  = {} vs uncompressed
        // length"
        //           + " {}",
        //       decompressed.length,
        //       statedUncompressedByteSize);
        //   linesToLog--;
        // }

        return new ByteArrayInputStream(decompressed);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
