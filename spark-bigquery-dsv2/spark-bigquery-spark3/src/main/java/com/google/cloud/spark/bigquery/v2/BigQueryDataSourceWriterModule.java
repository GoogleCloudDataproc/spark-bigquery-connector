package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDataSourceWriterModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

class BigQueryDataSourceWriterModule extends GenericBigQueryDataSourceWriterModule
    implements Module {

  private final StructType sparkSchema;
  private final SaveMode mode;

  BigQueryDataSourceWriterModule(String writeUUID, StructType sparkSchema, SaveMode mode) {
    super(writeUUID);
    this.sparkSchema = sparkSchema;
    this.mode = mode;
  }

  @Override
  public void configure(Binder binder) {
    // empty
  }

  @Singleton
  @Provides
  public BigQueryIndirectBatchWriter provideDataSourceWriter(
      BigQueryClient bigQueryClient, SparkBigQueryConfig config, SparkSession spark)
      throws IOException {
    Path gcsPath =
        createGcsPath(
            config,
            spark.sparkContext().hadoopConfiguration(),
            spark.sparkContext().applicationId());
    Optional<IntermediateDataCleaner> intermediateDataCleaner =
        config
            .getTemporaryGcsBucket()
            .map(
                ignored ->
                    new IntermediateDataCleaner(
                        gcsPath, spark.sparkContext().hadoopConfiguration()));
    // based on pmkc's suggestion at https://git.io/JeWRt
    intermediateDataCleaner.ifPresent(cleaner -> Runtime.getRuntime().addShutdownHook(cleaner));
    return new BigQueryIndirectBatchWriter(
        bigQueryClient,
        config,
        spark.sparkContext().hadoopConfiguration(),
        sparkSchema,
        getWriteUUID(),
        mode,
        gcsPath,
        intermediateDataCleaner);
  }
}
