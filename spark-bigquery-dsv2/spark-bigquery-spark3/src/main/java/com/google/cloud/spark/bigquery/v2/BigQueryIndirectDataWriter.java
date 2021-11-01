package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.AvroSchemaConverter;
import com.google.cloud.spark.bigquery.common.GenericBigQueryIndirectDataWriter;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigQueryIndirectDataWriter
    implements DataWriter<InternalRow> {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryIndirectDataWriter.class);
  FSDataOutputStream outputStream;
  StructType sparkSchema;
  IntermediateRecordWriter intermediateRecordWriter;

  protected BigQueryIndirectDataWriter(
      int partitionId,
      Path path,
      FileSystem fs,
      StructType sparkSchema,
      Schema avroSchema,
      IntermediateRecordWriter intermediateRecordWriter) {
    //super(partitionId, path, fs, sparkSchema,avroSchema);

  }

  @Override
  public void write(InternalRow record) throws IOException {
   /* GenericRecord avroRecord =
        AvroSchemaConverter.sparkRowToAvroGenericData(record, sparkSchema, getAvroSchema());
    intermediateRecordWriter.write(avroRecord);*/
  }

  @Override
  public void abort() throws IOException {
    //logger.warn(
      //  "Writing of partition {} has been aborted, attempting to delete {}",
/*        getPartitionId(),
        getPath());
    getFs().delete(getPath(), false);*/
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    intermediateRecordWriter.close();
    return new BigQueryIndirectWriterCommitMessage("");
  }

  @Override
  public void close() throws IOException {
/*    logger.warn("Closing File System", getPartitionId(), getPath());
    getFs().close();*/
  }
}
