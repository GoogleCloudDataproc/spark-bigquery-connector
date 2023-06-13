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

import com.google.cloud.spark.bigquery.write.context.DataSourceWriterContext;
import com.google.inject.Injector;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.V1WriteBuilder;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.InsertableRelation;

public class BigQueryWriteBuilder implements WriteBuilder, SupportsOverwrite,
    SupportsDynamicOverwrite, V1WriteBuilder {
  private Injector injector;
  private LogicalWriteInfo info;
  private SaveMode mode;

  private boolean overwriteDynamicPartitions = false;

  public BigQueryWriteBuilder(Injector injector, LogicalWriteInfo info, SaveMode mode) {
    this.injector = injector;
    this.info = info;
    this.mode = mode;
  }

  @Override
  public BatchWrite buildForBatch() {
    Optional<DataSourceWriterContext> dataSourceWriterContext =
        DataSourceWriterContext.create(
            injector, info.queryId(), info.schema(), mode, info.options());
    return new BigQueryBatchWrite(dataSourceWriterContext.get());
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    return null;
  }

  @Override
  public WriteBuilder truncate() {
    return new BigQueryWriteBuilder(injector, info, SaveMode.Overwrite);
  }

  @Override
  public WriteBuilder overwriteDynamicPartitions() {
    this.overwriteDynamicPartitions = true;
    return this;
  }

  @Override
  public InsertableRelation buildForV1Write() {
    return null;
  }
}
