package com.google.cloud.spark.bigquery.v2;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

public class BigQueryDirectDataSourceWriterModule implements com.google.inject.Module {

  final String writeUUID;
  final SaveMode saveMode;
  final StructType sparkSchema;

  public BigQueryDirectDataSourceWriterModule(
      String writeUUID, SaveMode saveMode, StructType sparkSchema) {
    this.writeUUID = writeUUID;
    this.saveMode = saveMode;
    this.sparkSchema = sparkSchema;
  }

  @Override
  public void configure(Binder binder) {
    // BigQuery related
  }

  @Singleton
  @Provides
  public BigQueryDirectDataSourceWriter provideDataSourceWriter(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      SparkBigQueryConfig config) {
    TableId tableId = config.getTableId();
    RetrySettings bigqueryDataWriteHelperRetrySettings =
        config.getBigqueryDataWriteHelperRetrySettings();
    return new BigQueryDirectDataSourceWriter(
        bigQueryClient,
        bigQueryWriteClientFactory,
        tableId,
        writeUUID,
        saveMode,
        sparkSchema,
        bigqueryDataWriteHelperRetrySettings);
  }
}
