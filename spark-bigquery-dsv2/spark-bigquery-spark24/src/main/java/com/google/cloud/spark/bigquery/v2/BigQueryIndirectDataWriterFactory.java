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

import com.google.cloud.spark.bigquery.common.GenericBigQueryIndirectDataWriterFactory;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

class BigQueryIndirectDataWriterFactory implements DataWriterFactory<InternalRow>, Serializable {
  private final GenericBigQueryIndirectDataWriterFactory dataWriterFactory;

  public BigQueryIndirectDataWriterFactory(
      SerializableConfiguration conf,
      String gcsDirPath,
      StructType sparkSchema,
      String avroSchemaJson) {
    this.dataWriterFactory =
        new GenericBigQueryIndirectDataWriterFactory(conf, gcsDirPath, sparkSchema, avroSchemaJson);
  }

  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
    try {
      this.dataWriterFactory.enableDataWriter(partitionId, taskId, epochId);
      return new BigQueryIndirectDataWriter(
          partitionId,
          this.dataWriterFactory.getPath(),
          this.dataWriterFactory.getFs(),
          this.dataWriterFactory.getSparkSchema(),
          this.dataWriterFactory.getAvroSchema(),
          this.dataWriterFactory.getIntermediateRecordWriter());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
