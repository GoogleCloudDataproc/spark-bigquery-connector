/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.io.UncheckedIOException;

public class BigQueryIndirectDataWriterFactory implements DataWriterFactory<InternalRow> {

  SerializableConfiguration conf;
  String gcsDirPath;
  StructType sparkSchema;
  String avroSchemaJson;
  SparkBigQueryConfig.IntermediateFormat intermediateFormat;

  public BigQueryIndirectDataWriterFactory(
      SerializableConfiguration conf,
      String gcsDirPath,
      StructType sparkSchema,
      String avroSchemaJson,
      SparkBigQueryConfig.IntermediateFormat intermediateFormat) {
    this.conf = conf;
    this.gcsDirPath = gcsDirPath;
    this.sparkSchema = sparkSchema;
    this.avroSchemaJson = avroSchemaJson;
    this.intermediateFormat = intermediateFormat;
  }

  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
    try {
      Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
      String uri =
          String.format(
              "%s/part-%06d.%s",
              gcsDirPath, partitionId, intermediateFormat.toString().toLowerCase());
      Path path = new Path(uri);
      FileSystem fs = path.getFileSystem(conf.value());
      IntermediateRecordWriter intermediateRecordWriter =
          createIntermediateRecordWriter(avroSchema, path, fs);
      return new BigQueryIndirectDataWriter(
          partitionId, path, fs, sparkSchema, avroSchema, intermediateRecordWriter);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  IntermediateRecordWriter createIntermediateRecordWriter(
      Schema avroSchema, Path path, FileSystem fs) throws IOException {
    switch (intermediateFormat) {
      case AVRO:
        return new AvroIntermediateRecordWriter(avroSchema, fs.create(path));
      case PARQUET:
        return new ParquetIntermediateRecordWriter(avroSchema, path, conf.value());
      default:
        throw new IllegalArgumentException(
            "Only AVRO and PARQUET intermediate formats are supported");
    }
  }
}
