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

import com.google.cloud.spark.bigquery.v2.context.DataWriterContext;
import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class Spark31BigQueryDataWriter implements DataWriter<InternalRow> {

  private DataWriterContext<InternalRow> ctx;

  public Spark31BigQueryDataWriter(DataWriterContext<InternalRow> ctx) {
    this.ctx = ctx;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    ctx.write(record);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    return new Spark31BigQueryWriterCommitMessage(ctx.commit());
  }

  @Override
  public void abort() throws IOException {
    ctx.abort();
  }

  @Override
  public void close() throws IOException {
    ctx.close();
  }
}
