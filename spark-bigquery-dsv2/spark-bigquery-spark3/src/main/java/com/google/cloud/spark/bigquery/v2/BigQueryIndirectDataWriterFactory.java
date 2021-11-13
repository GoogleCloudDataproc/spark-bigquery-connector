package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.common.GenericBigQueryIndirectDataWriterFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

class BigQueryIndirectDataWriterFactory implements DataWriterFactory {
  GenericBigQueryIndirectDataWriterFactory dataWriterFactoryHelper;

  public BigQueryIndirectDataWriterFactory(
      SerializableConfiguration conf,
      String gcsDirPath,
      StructType sparkSchema,
      String avroSchemaJson) {
    this.dataWriterFactoryHelper =
        new GenericBigQueryIndirectDataWriterFactory(conf, gcsDirPath, sparkSchema, avroSchemaJson);
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    try {
      this.dataWriterFactoryHelper.enableDataWriter(partitionId, taskId, -1L);
      return new BigQueryIndirectDataWriter(
          partitionId,
          this.dataWriterFactoryHelper.getPath(),
          this.dataWriterFactoryHelper.getFs(),
          this.dataWriterFactoryHelper.getSparkSchema(),
          this.dataWriterFactoryHelper.getAvroSchema(),
          this.dataWriterFactoryHelper.getIntermediateRecordWriter());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
