package com.google.cloud.spark.bigquery.common;

import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;

public class GenericBigQueryIndirectDataWriterFactory {
  String gcsDirPath;
  String avroSchemaJson;
  String uri;
  Path path;
  Schema avroSchema;

  public GenericBigQueryIndirectDataWriterFactory(String gcsDirPath, String avroSchemaJson) {
    this.gcsDirPath = gcsDirPath;
    this.avroSchemaJson = avroSchemaJson;
  }

  public String getAvroSchemaJson() {
    return this.avroSchemaJson;
  }

  public String getGcsDirPath() {
    return this.gcsDirPath;
  }

  public Path getPath() {
    return path;
  }

  public Schema getAvroSchema() {
    return avroSchema;
  }

  public void enableDataWriter(int partitionId, long taskId, long epochId) {
    this.avroSchema = new Schema.Parser().parse(this.avroSchemaJson);
    UUID uuid;
    if (epochId != -1L) {
      uuid = new UUID(taskId, epochId);
    } else {
      uuid = new UUID(taskId, partitionId);
    }
    this.uri = String.format("%s/part-%06d-%s.avro", this.gcsDirPath, partitionId, uuid);
    this.path = new Path(uri);
  }
}
