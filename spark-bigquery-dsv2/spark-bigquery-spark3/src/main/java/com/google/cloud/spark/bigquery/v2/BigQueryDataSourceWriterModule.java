package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDataSourceWriterModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.IOException;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.StructType;

class BigQueryDataSourceWriterModule implements Module {
  private GenericBigQueryDataSourceWriterModule dataSourceWriterModuleHelper;
  private final LogicalWriteInfo logicalWriteInfo;
  private final BigQueryClient bigQueryClient;
  private final SparkBigQueryConfig config;
  private SaveMode mode;

  BigQueryDataSourceWriterModule(
      String writeUUID,
      StructType sparkSchema,
      SaveMode mode,
      LogicalWriteInfo logicalWriteInfo,
      BigQueryClient bigQueryClient,
      SparkBigQueryConfig config) {
    this.mode = mode;
    this.dataSourceWriterModuleHelper =
        new GenericBigQueryDataSourceWriterModule(writeUUID, sparkSchema, this.mode);
    this.logicalWriteInfo = logicalWriteInfo;
    this.bigQueryClient = bigQueryClient;
    this.config = config;
  }

  @Override
  public void configure(Binder binder) {
    // empty
  }

  @Singleton
  @Provides
  public BigQueryWriteBuilder provideDataSourceWriter(
      BigQueryClient bigQueryClient, SparkBigQueryConfig config, SparkSession spark)
      throws IOException {
    this.dataSourceWriterModuleHelper.createIntermediateCleaner(
        config, spark.sparkContext().hadoopConfiguration(), spark.sparkContext().applicationId());
    return new BigQueryWriteBuilder(
        bigQueryClient,
        config,
        spark.sparkContext().hadoopConfiguration(),
        this.dataSourceWriterModuleHelper.getSparkSchema(),
        this.dataSourceWriterModuleHelper.getWriteUUID(),
        this.dataSourceWriterModuleHelper.getMode(),
        this.dataSourceWriterModuleHelper.getGcsPath(),
        this.dataSourceWriterModuleHelper.getIntermediateDataCleaner(),
        this.logicalWriteInfo);
  }
}
