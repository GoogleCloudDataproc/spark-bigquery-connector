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

import com.google.cloud.spark.bigquery.InjectorFactory;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderModule;
import com.google.cloud.spark.bigquery.write.context.DataSourceWriterContext;
import com.google.inject.Injector;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

/**
 * A DataSourceV2 implementation, providing efficient reader and writer for the Google Cloud
 * Platform BigQuery.
 */
public class BigQueryDataSourceV2 extends BaseBigQuerySource
    implements DataSourceV2, ReadSupport, WriteSupport {

  @Override
  public DataSourceReader createReader(StructType schema, DataSourceOptions options) {
    Injector injector =
        InjectorFactory.createInjector(schema, options.asMap(), /* tableIsMandatory */ true)
            .createChildInjector(new BigQueryDataSourceReaderModule());
    BigQueryDataSourceReader reader =
        new BigQueryDataSourceReader(injector.getInstance(BigQueryDataSourceReaderContext.class));
    return reader;
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    return createReader(null, options);
  }

  /**
   * Returning a DataSourceWriter for the specified parameters. In case the table already exist and
   * the SaveMode is "Ignore", an Optional.empty() is returned.
   */
  @Override
  public Optional<DataSourceWriter> createWriter(
      String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
    Injector injector =
        InjectorFactory.createInjector(schema, options.asMap(), /* tableIsMandatory */ true);
    Optional<DataSourceWriterContext> dataSourceWriterContext =
        DataSourceWriterContext.create(injector, writeUUID, schema, mode, options.asMap());
    return dataSourceWriterContext.map(ctx -> new BigQueryDataSourceWriter(ctx));
  }
}
