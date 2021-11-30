package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDirectDataSourceWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDirectBatchWriter implements BatchWrite {

  final Logger logger = LoggerFactory.getLogger(BigQueryDirectBatchWriter.class);
  private GenericBigQueryDirectDataSourceWriter dataSourceWriterHelper;

  public BigQueryDirectBatchWriter(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      TableId destinationTableId,
      String writeUUID,
      SaveMode saveMode,
      StructType sparkSchema,
      SparkBigQueryConfig config) {
    this.dataSourceWriterHelper =
        new GenericBigQueryDirectDataSourceWriter(
            bigQueryClient,
            bigQueryWriteClientFactory,
            destinationTableId,
            writeUUID,
            saveMode,
            sparkSchema,
            config.getBigqueryDataWriteHelperRetrySettings());
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
    return new BigQueryDirectDataWriterFactory(
        this.dataSourceWriterHelper.getWriteClientFactory(),
        this.dataSourceWriterHelper.getTablePathForBigQueryStorage(),
        this.dataSourceWriterHelper.getSparkSchema(),
        this.dataSourceWriterHelper.getProtoSchema(),
        this.dataSourceWriterHelper.evaluateIfIgnoreInput(),
        this.dataSourceWriterHelper.getBigqueryDataWriterHelperRetrySettings());
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {
    BatchWrite.super.onDataWriterCommit(message);
  }

  @Override
  public void commit(WriterCommitMessage[] writerCommitMessages) {
    if (this.dataSourceWriterHelper.evaluateIfIgnoreInput()) return;
    List<String> streamNames = new ArrayList<String>();
    for (WriterCommitMessage message : writerCommitMessages) {
      streamNames.add(((BigQueryDirectWriterCommitMessage) message).getWriteStreamName());
    }
    this.dataSourceWriterHelper.commit(streamNames, logger);
  }

  @Override
  public void abort(WriterCommitMessage[] writerCommitMessages) {
    this.dataSourceWriterHelper.abort(logger);
  }
}
