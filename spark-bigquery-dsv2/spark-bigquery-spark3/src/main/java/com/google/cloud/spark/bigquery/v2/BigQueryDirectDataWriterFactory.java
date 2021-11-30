package com.google.cloud.spark.bigquery.v2;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.storage.v1beta2.ProtoSchema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class BigQueryDirectDataWriterFactory implements DataWriterFactory {

  private final BigQueryClientFactory writeClientFactory;
  private final String tablePath;
  private final StructType sparkSchema;
  private final ProtoSchema protoSchema;
  private final boolean ignoreInputs;
  private final RetrySettings bigqueryDataWriterHelperRetrySettings;

  public BigQueryDirectDataWriterFactory(
      BigQueryClientFactory writeClientFactory,
      String tablePath,
      StructType sparkSchema,
      ProtoSchema protoSchema,
      boolean ignoreInputs,
      RetrySettings bigqueryDataWriterHelperRetrySettings) {
    this.writeClientFactory = writeClientFactory;
    this.tablePath = tablePath;
    this.sparkSchema = sparkSchema;
    this.protoSchema = protoSchema;
    this.ignoreInputs = ignoreInputs;
    this.bigqueryDataWriterHelperRetrySettings = bigqueryDataWriterHelperRetrySettings;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    if (this.ignoreInputs) {
      return new NoOpDataWriter();
    }
    return new BigQueryDirectDataWriter(
        partitionId,
        taskId,
        this.writeClientFactory,
        this.tablePath,
        this.sparkSchema,
        this.protoSchema,
        this.bigqueryDataWriterHelperRetrySettings);
  }
}
