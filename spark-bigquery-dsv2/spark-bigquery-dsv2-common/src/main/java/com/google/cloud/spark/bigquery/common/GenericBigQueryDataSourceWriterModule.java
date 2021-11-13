package com.google.cloud.spark.bigquery.common;

import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

public class GenericBigQueryDataSourceWriterModule implements Serializable {
  private final String writeUUID;
  private final StructType sparkSchema;
  private final SaveMode mode;
  private Optional<IntermediateDataCleaner> intermediateDataCleaner;
  private Path gcsPath;

  public GenericBigQueryDataSourceWriterModule(
      String writeUUID, StructType sparkSchema, SaveMode mode) {
    this.writeUUID = writeUUID;
    this.sparkSchema = sparkSchema;
    this.mode = mode;
  }

  public String getWriteUUID() {
    return writeUUID;
  }

  public Path createGcsPath(SparkBigQueryConfig config, Configuration conf, String applicationId)
      throws IOException {
    Preconditions.checkArgument(
        config.getTemporaryGcsBucket().isPresent() || config.getPersistentGcsBucket().isPresent(),
        "Temporary or persistent GCS bucket must be informed.");
    boolean needNewPath = true;
    Path gcsPath = null;
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
                    return String.format("gs://%s/%s", config.getPersistentGcsBucket().get(), path);
                  });
      gcsPath = new Path(gcsPathOption);
      FileSystem fs = gcsPath.getFileSystem(conf);
      needNewPath = fs.exists(gcsPath); // if the path exists for some reason, then retry
    }
    return gcsPath;
  }

  public void createIntermediateCleaner(
      SparkBigQueryConfig config, Configuration conf, String applicationId) throws IOException {
    this.gcsPath = createGcsPath(config, conf, applicationId);
    this.intermediateDataCleaner =
        config.getTemporaryGcsBucket().map(ignored -> new IntermediateDataCleaner(gcsPath, conf));
    // based on pmkc's suggestion at https://git.io/JeWRt
    this.intermediateDataCleaner.ifPresent(
        cleaner -> Runtime.getRuntime().addShutdownHook(cleaner));
  }

  public StructType getSparkSchema() {
    return sparkSchema;
  }

  public SaveMode getMode() {
    return mode;
  }

  public Optional<IntermediateDataCleaner> getIntermediateDataCleaner() {
    return intermediateDataCleaner;
  }

  public Path getGcsPath() {
    return gcsPath;
  }
}
