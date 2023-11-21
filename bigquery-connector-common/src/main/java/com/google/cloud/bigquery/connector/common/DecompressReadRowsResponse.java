package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

// import io.netty.handler.codec.compression.Snappy;

public class DecompressReadRowsResponse {
  private static long linesToLog = 100; // reduce spam only return this number of lines
  private static final Logger log = LoggerFactory.getLogger(DecompressReadRowsResponse.class);

  /** Returns a new allocator limited to maxAllocation bytes */
  public static InputStream decompressArrowRecordBatch(ReadRowsResponse response) {

    InputStream inputStream = response.getArrowRecordBatch().getSerializedRecordBatch().newInput();

    // UnknownFieldSet unknownFieldSet = response.getUnknownFields();
    // java.util.Map<Integer, UnknownFieldSet.Field> unknownFieldSetMap = unknownFieldSet.asMap();
    // if (linesToLog > 0) {
    //   log.info(
    //       "AQIU: DecompressReadRowsResponse ReadRowsResponse UnknownFieldSet.asMap {} lines to
    // log"
    //           + " {}",
    //       unknownFieldSetMap,
    //       linesToLog);

    //   for (Integer key : unknownFieldSetMap.keySet()) {
    //     UnknownFieldSet.Field value = unknownFieldSetMap.get(key);
    //     log.info(
    //         "AQIU: DecompressReadRowsResponse UnknownFieldSetMap[{}] = {}, varint {}, int32 {},
    // int"
    //             + " 64 {} , group {} , length delimited {} toString()",
    //         key,
    //         value,
    //         value.getVarintList(),
    //         value.getFixed32List(),
    //         value.getFixed64List(),
    //         value.getGroupList(),
    //         value.getLengthDelimitedList(),
    //         value.toString());
    //   }
    //   log.info(
    //       "AQIU: DecompressReadRowsResponse 9 = {}",
    //       unknownFieldSet.getField(9).getVarintList().get(0));
    //   linesToLog--;
    // }

    long statedUncompressedByteSize =
        response.getUnknownFields().getField(9).getVarintList().get(0);

    if (statedUncompressedByteSize <= 0) {
      // if (linesToLog > 0) {
      //   log.info(
      //       "AQIU: DecompressReadRowsResponse found uncompressedByteSize is 0 or less. it is {}",
      //       statedUncompressedByteSize);
      //   linesToLog--;
      // }

      // no compession here
      return inputStream;
    }

    try {

      // Note that if trying to use asReadOnlyByteBuffer() to avoid copying to Byte[] bY using the
      // same backing array as the byte string, if possible, then
      // Snappy.uncompress(response.getArrowRecordBatch().getSerializedRecordBatch().asReadOnlyByteBuffer(),arrowRecordBatchBuffer)
      // does not work because
      // response.getArrowRecordBatch().getSerializedRecordBatch().asReadOnlyByteBuffer() is not a
      // direct buffer, and snappy will give this error:
      // com.google.cloud.spark.bigquery.repackaged.org.xerial.snappy.SnappyError:
      // [NOT_A_DIRECT_BUFFER] input is not a direct buffer

      if (linesToLog > 0) {
        log.info(
            "AQIU: DecompressReadRowsResponse bytebuffer.isdirect  = {}",
            response
                .getArrowRecordBatch()
                .getSerializedRecordBatch()
                .asReadOnlyByteBuffer()
                .isDirect());
        linesToLog--;
      }
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
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
