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

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryConnectorModule;
import com.google.cloud.spark.bigquery.common.GenericDataSourceHelperClass;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
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



  @Override
  public DataSourceReader createReader(StructType schema, DataSourceOptions options) {
    Injector injector = createInjector(schema, options.asMap(), new BigQueryDataSourceReaderModule());
    BigQueryDataSourceReader reader = injector.getInstance(BigQueryDataSourceReader.class);
    return reader;
  }
  // This method is used to create spark session
  public static SparkSession getDefaultSparkSessionOrCreate() {
    scala.Option<SparkSession> defaultSpareSession = SparkSession.getActiveSession();
    if (defaultSpareSession.isDefined()) {
      return defaultSpareSession.get();
    }
    return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
  }
  // This method is used to create injection by providing
  public static Injector createInjector(StructType schema, Map<String, String> options, Module module) {
    SparkSession spark = getDefaultSparkSessionOrCreate();
    return Guice.createInjector(
            new BigQueryClientModule(),
            new SparkBigQueryConnectorModule(
                    spark, options, Optional.ofNullable(schema), DataSourceVersion.V2),
            module);
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
        createInjector(
            schema, options.asMap(), new BigQueryDataSourceWriterModule(writeUUID, schema, mode));
    // first verify if we need to do anything at all, based on the table existence and the save
    // mode.
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    TableInfo table = bigQueryClient.getTable(config.getTableId());
    if (table != null) {
      // table already exists
      if (mode == SaveMode.Ignore) {
        return Optional.empty();
      }
      if (mode == SaveMode.ErrorIfExists) {
        throw new IllegalArgumentException(
            String.format(
                "SaveMode is set to ErrorIfExists and table '%s' already exists. Did you want "
                    + "to add data to the table by setting the SaveMode to Append? Example: "
                    + "df.write.format.options.mode(\"append\").save()",
                BigQueryUtil.friendlyTableName(table.getTableId())));
      }
    } else {
      // table does not exist
      // If the CreateDisposition is CREATE_NEVER, and the table does not exist,
      // there's no point in writing the data to GCS in the first place as it going
      // to fail on the BigQuery side.
          new GenericDataSourceHelperClass().checkCreateDisposition(config);
    }
    BigQueryIndirectDataSourceWriter writer =
        injector.getInstance(BigQueryIndirectDataSourceWriter.class);
    return Optional.of(writer);
  }

  @Override
  public String shortName() {
    return "bigquery";
  }
}
