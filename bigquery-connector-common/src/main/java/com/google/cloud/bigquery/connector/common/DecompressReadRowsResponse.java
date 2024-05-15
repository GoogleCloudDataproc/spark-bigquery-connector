package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class DecompressReadRowsResponse {

  public static byte[] decompressArrowRecordBatch(
      ReadRowsResponse response, ResponseCompressionCodec compressionCodec) throws IOException {
    byte[] responseBytes = response.getArrowRecordBatch().getSerializedRecordBatch().toByteArray();
    return decompressRecordBatchInternal(response, compressionCodec, responseBytes);
  }
  public static byte[] decompressAvroRecordBatch(
      ReadRowsResponse response, ResponseCompressionCodec compressionCodec) throws IOException {
    byte[] responseBytes = response.getAvroRows().getSerializedBinaryRows().toByteArray();
    return decompressRecordBatchInternal(response, compressionCodec, responseBytes);
  }

  private static byte[] decompressRecordBatchInternal(ReadRowsResponse response,
      ResponseCompressionCodec compressionCodec, byte[] responseBytes) throws IOException {
    // step 1: read the uncompressed_byte_size
    // https://cloud.google.com/java/docs/reference/google-cloud-bigquerystorage/latest/com.google.cloud.bigquery.storage.v1.ReadRowsResponse#com_google_cloud_bigquery_storage_v1_ReadRowsResponse_getUncompressedByteSize__
    //
    long uncompressedByteSize = response.getUncompressedByteSize();
    if (uncompressedByteSize <= 0) {
      // response was not compressed, return directly
      return responseBytes;
    }

    // step 2: decompress using the compression codec
    // If the uncompressed byte size is set, then we want to decompress it, using the specified
    // compression codec.
    switch (compressionCodec) {
      case RESPONSE_COMPRESSION_CODEC_LZ4:
        // decompress LZ4
        // https://github.com/lz4/lz4-java
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        byte[] decompressed = new byte[(int) uncompressedByteSize];
        decompressor.decompress(responseBytes, 0, decompressed, 0, (int) uncompressedByteSize);
        return decompressed;
      case RESPONSE_COMPRESSION_CODEC_UNSPECIFIED:
      default:
        // error! the response claims that it was compressed, but you did not specify which
        // compression codec to use.
        throw new IOException(
            "Missing a compression codec to decode a compressed ReadRowsResponse");
    }
  }
}
