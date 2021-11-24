/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
import java.io.UncheckedIOException;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

class BigQueryIndirectDataWriterFactory implements DataWriterFactory {
  GenericBigQueryIndirectDataWriterFactory dataWriterFactoryHelper;

  public BigQueryIndirectDataWriterFactory(
      SerializableConfiguration conf,
      String gcsDirPath,
      StructType sparkSchema,
      String avroSchemaJson) {
    this.dataWriterFactoryHelper =
        new GenericBigQueryIndirectDataWriterFactory(conf, gcsDirPath, sparkSchema, avroSchemaJson);
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    try {
      this.dataWriterFactoryHelper.enableDataWriter(partitionId, taskId, -1L);
      return new BigQueryIndirectDataWriter(
          partitionId,
          this.dataWriterFactoryHelper.getPath(),
          this.dataWriterFactoryHelper.getFs(),
          this.dataWriterFactoryHelper.getSparkSchema(),
          this.dataWriterFactoryHelper.getAvroSchema(),
          this.dataWriterFactoryHelper.getIntermediateRecordWriter());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
