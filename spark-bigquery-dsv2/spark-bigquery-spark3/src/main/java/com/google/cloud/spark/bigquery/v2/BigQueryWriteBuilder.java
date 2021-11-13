package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.common.GenericBigQueryIndirectDataSourceWriter;
import com.google.cloud.spark.bigquery.common.IntermediateDataCleaner;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public class BigQueryWriteBuilder implements WriteBuilder, SupportsOverwrite {
  private GenericBigQueryIndirectDataSourceWriter dataSourceWriterHelper;
  private final LogicalWriteInfo logicalWriteInfo;

  public BigQueryWriteBuilder(
      BigQueryClient bigQueryClient,
      SparkBigQueryConfig config,
      Configuration conf,
      StructType sparkSchema,
      String writeUUID,
      SaveMode mode,
      Path gcsPath,
      Optional<IntermediateDataCleaner> intermediateDataCleaner,
      LogicalWriteInfo logicalWriteInfo) {
    this.logicalWriteInfo = logicalWriteInfo;
    this.dataSourceWriterHelper =
        new GenericBigQueryIndirectDataSourceWriter(
            bigQueryClient,
            config,
            conf,
            sparkSchema,
            writeUUID,
            mode,
            gcsPath,
            intermediateDataCleaner);
  }

  @Override
  public BatchWrite buildForBatch() {
    return new BigQueryIndirectBatchWriter(
        this.dataSourceWriterHelper.getBigQueryClient(),
        this.dataSourceWriterHelper.getConfig(),
        this.dataSourceWriterHelper.getHadoopConfiguration(),
        this.dataSourceWriterHelper.getSparkSchema(),
        this.dataSourceWriterHelper.getWriteUUID(),
        this.dataSourceWriterHelper.getSaveMode(),
        this.dataSourceWriterHelper.getGcsPath(),
        this.dataSourceWriterHelper.getIntermediateDataCleaner(),
        this.logicalWriteInfo);
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    this.dataSourceWriterHelper.setMode(SaveMode.Overwrite);
    return this;
  }
}
