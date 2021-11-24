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

import com.google.cloud.spark.bigquery.common.BigQueryDataSourceHelper;
import com.google.inject.Injector;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.DataSourceRegister;
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
public class BigQueryDataSourceV2
    implements DataSourceV2, DataSourceRegister, ReadSupport, WriteSupport {

  private BigQueryDataSourceHelper bigQueryDataSourceHelper = new BigQueryDataSourceHelper();

  //  private enum WriteMethod {
  //    DIRECT("direct"),
  //    INDIRECT("indirect");
  //
  //    private final String writePath;
  //
  //    WriteMethod(String writePath) {
  //      this.writePath = writePath;
  //    }
  //
  //    static WriteMethod getWriteMethod(Optional<String> path) {
  //      if (!path.isPresent() || path.get().equalsIgnoreCase("direct")) {
  //        return DIRECT;
  //      } else if (path.get().equalsIgnoreCase("indirect")) {
  //        return INDIRECT;
  //      } else {
  //        throw new IllegalArgumentException("Unknown writePath Provided for writing the
  // DataFrame");
  //      }
  //    }
  //  }

  @Override
  public DataSourceReader createReader(StructType schema, DataSourceOptions options) {
    Injector injector =
        this.bigQueryDataSourceHelper.createInjector(
            schema, options, null, new BigQueryDataSourceReaderModule());
    BigQueryDataSourceReader reader = injector.getInstance(BigQueryDataSourceReader.class);
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
    if (this.bigQueryDataSourceHelper.isDirectWrite(options.get("writePath"))) {
      return createDirectDataSourceWriter(writeUUID, schema, mode, options);
    } else {
      return createIndirectDataSourceWriter(writeUUID, schema, mode, options);
    }
  }

  private Optional<DataSourceWriter> createDirectDataSourceWriter(
      String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
    Injector injector =
        this.bigQueryDataSourceHelper.createInjector(
            schema,
            options,
            mode,
            new BigQueryDirectDataSourceWriterModule(writeUUID, mode, schema));

    BigQueryDirectDataSourceWriter writer =
        injector.getInstance(BigQueryDirectDataSourceWriter.class);
    return Optional.of(writer);
  }

  private Optional<DataSourceWriter> createIndirectDataSourceWriter(
      String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
    Injector injector =
        bigQueryDataSourceHelper.createInjector(
            schema, options, mode, new BigQueryDataSourceWriterModule(writeUUID, schema, mode));
    BigQueryIndirectDataSourceWriter writer =
        injector.getInstance(BigQueryIndirectDataSourceWriter.class);
    return Optional.of(writer);
  }

  @Override
  public String shortName() {
    return "bigquery";
  }
}
