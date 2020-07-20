package com.google.cloud.bigquery.connector.common;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.v2.BigQueryDataSourceWriter;
import com.google.inject.*;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class BigQueryWriteClientModule implements Module {

  final String writeUUID;
  final SaveMode saveMode;
  final StructType sparkSchema;

  public BigQueryWriteClientModule(String writeUUID, SaveMode saveMode, StructType sparkSchema) {
    this.writeUUID = writeUUID;
    this.saveMode = saveMode;
    this.sparkSchema = sparkSchema;
  }

  @Override
  public void configure(Binder binder) {
    // BigQuery related
    binder.bind(BigQueryWriteClientFactory.class).in(Scopes.SINGLETON);
  }

  @Singleton
  @Provides
  public BigQueryDataSourceWriter provideDataSourceWriter(
      BigQueryClient bigQueryClient,
      BigQueryWriteClientFactory bigQueryWriteClientFactory,
      SparkBigQueryConfig config) {
    TableId tableId = config.getTableId();
    return new BigQueryDataSourceWriter(
        bigQueryClient, bigQueryWriteClientFactory, tableId, writeUUID, saveMode, sparkSchema);
  }
}
