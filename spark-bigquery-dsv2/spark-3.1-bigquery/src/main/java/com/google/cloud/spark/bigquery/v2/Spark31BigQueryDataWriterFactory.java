/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.write.context.DataWriterContextFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;

public class Spark31BigQueryDataWriterFactory implements DataWriterFactory {

  private DataWriterContextFactory<InternalRow> writerContextFactory;

  public Spark31BigQueryDataWriterFactory(
      DataWriterContextFactory<InternalRow> writerContextFactory) {
    this.writerContextFactory = writerContextFactory;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new Spark31BigQueryDataWriter(
        writerContextFactory.createDataWriterContext(partitionId, taskId, 0));
  }
}
