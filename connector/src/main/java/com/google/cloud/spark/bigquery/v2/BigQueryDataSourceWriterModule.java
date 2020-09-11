package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.UUID;

public class BigQueryDataSourceWriterModule implements Module {

  private final String writeUUID;
  private final StructType sparkSchema;
  private final SaveMode mode;

  BigQueryDataSourceWriterModule(String writeUUID, StructType sparkSchema, SaveMode mode) {
    this.writeUUID = writeUUID;
    this.sparkSchema = sparkSchema;
    this.mode = mode;
  }

  @Override
  public void configure(Binder binder) {
    // empty
  }

  @Singleton
  @Provides
  public BigQueryIndirectDataSourceWriter provideDataSourceWriter(
      BigQueryClient bigQueryClient, SparkBigQueryConfig config, SparkSession spark) {
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
    return new BigQueryIndirectDataSourceWriter(
        bigQueryClient,
        config,
        spark.sparkContext().hadoopConfiguration(),
        sparkSchema,
        writeUUID,
        mode,
        gcsPath,
        intermediateDataCleaner);
  }

  Path createGcsPath(SparkBigQueryConfig config, Configuration conf, String applicationId) {
    Preconditions.checkArgument(
        config.getTemporaryGcsBucket().isPresent() || config.getPersistentGcsBucket().isPresent(),
        "Temporary or persistent GCS bucket must be informed.");
    boolean needNewPath = true;
    Path gcsPath = null;
    try {
      while (needNewPath) {
        String gcsPathOption =
            config
                .getTemporaryGcsBucket()
                .map(
                    bucket ->
                        String.format(
                            "gs://%s/.spark-bigquery-%s-%s",
                            bucket, applicationId, UUID.randomUUID()))
                .orElseGet(
                    () -> {
                      // if we are here it means that the PersistentGcsBucket is set
                      String path =
                          config
                              .getPersistentGcsPath()
                              .orElse(
                                  String.format(
                                      ".spark-bigquery-%s-%s", applicationId, UUID.randomUUID()));
                      return String.format(
                          "gs://%s/%s", config.getPersistentGcsBucket().get(), path);
                    });
        gcsPath = new Path(gcsPathOption);
        FileSystem fs = gcsPath.getFileSystem(conf);
        needNewPath = fs.exists(gcsPath); // if the path exists for some reason, then retry
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return gcsPath;
  }
}
