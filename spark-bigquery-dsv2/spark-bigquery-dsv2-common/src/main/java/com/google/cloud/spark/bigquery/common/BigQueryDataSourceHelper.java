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
package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryConnectorModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/** The class acts as a helper class to set up DSV2 for initiating the read and write processes */
public class BigQueryDataSourceHelper {

  private boolean isDirectWrite = false;
  private StructType schema;
  private boolean tableExists = false;

  // enum to track type of write
  private enum WriteMethod {
    DIRECT("direct"),
    INDIRECT("indirect");

    private final String writePath;

    WriteMethod(String writePath) {
      this.writePath = writePath;
    }

    /**
     * Method to extract the write type from options
     *
     * @param path option passed by the end-user during write
     * @return the write method
     */
    static WriteMethod getWriteMethod(Optional<String> path) {
      if (!path.isPresent() || path.get().equalsIgnoreCase("direct")) {
        return DIRECT;
      } else if (path.get().equalsIgnoreCase("indirect")) {
        return INDIRECT;
      } else {
        throw new IllegalArgumentException("Unknown writePath Provided for writing the DataFrame");
      }
    }

    /**
     * Method to extract the write type from options
     *
     * @param path option passed by the end-user during write
     * @return the write method
     */
    static WriteMethod getWriteMethod(String path) {
      if (path == null || path.equalsIgnoreCase("direct")) {
        return DIRECT;
      } else if (path.equalsIgnoreCase("indirect")) {
        return INDIRECT;
      } else {
        throw new IllegalArgumentException("Unknown writePath Provided for writing the DataFrame");
      }
    }
  }

  public StructType getSchema() {
    return schema;
  }

  /**
   * Method to initiate a new spark session or retrieve an existing spark session
   *
   * @return spark session for carrying out the operations
   */
  public SparkSession getDefaultSparkSessionOrCreate() {
    scala.Option<SparkSession> defaultSpareSession = SparkSession.getActiveSession();
    if (defaultSpareSession.isDefined()) {
      return defaultSpareSession.get();
    }
    return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
  }

  /**
   * Method to create an injector to help integration with Spark and BigQuery components, and to
   * initiate the model
   *
   * @param schema schema of the table
   * @param options options from the end-user for the spark request
   * @param isDirectWrite if the write type is direct or indirect
   * @param mode mode of the spark write
   * @param module module to be instantiated
   * @return injector to instantiate the module class
   */
  public Injector createInjector(
      StructType schema,
      Map<String, String> options,
      boolean isDirectWrite,
      SaveMode mode,
      Module module) {
    SparkSession spark = getDefaultSparkSessionOrCreate();
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new SparkBigQueryConnectorModule(
                spark, options, Optional.ofNullable(schema), DataSourceVersion.V2),
            module);
    return getTableInformation(injector, mode, isDirectWrite);
  }

  /**
   * Method to retrieve table information from BigQuery
   *
   * @param injector pre-constructed injector with dependencies of already-constructed instances
   * @param mode save mode of write, null if read
   * @param isDirectWrite indicates if it is a direct read or not
   * @return injector to instantiate the module class
   */
  public Injector getTableInformation(Injector injector, SaveMode mode, boolean isDirectWrite) {
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    TableInfo table = bigQueryClient.getTable(config.getTableId());
    if (table != null) {
      this.tableExists = true;
      if (mode != null && !isDirectWrite) {
        if (mode == SaveMode.Ignore) {
          //                    return Optional.empty();
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
        this.schema = SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table));
      }
    } else {
      // table does not exist
      // If the CreateDisposition is CREATE_NEVER, and the table does not exist,
      // there's no point in writing the data to GCS in the first place as it going
      // to fail on the BigQuery side.
      new GenericDataSourceHelperClass().checkCreateDisposition(config);
    }
    return injector;
  }

  /**
   * Method to evaluate the write type from option
   *
   * @param writeType indicates if the write type is direct or indirect
   * @return if the write request is a direct write request or not
   */
  public boolean isDirectWrite(Optional<String> writeType) {
    WriteMethod path = WriteMethod.getWriteMethod(writeType);
    if (path.equals(WriteMethod.DIRECT)) {
      this.isDirectWrite = true;
      return true;
    } else if (path.equals(WriteMethod.INDIRECT)) {
      this.isDirectWrite = false;
      return false;
    } else {
      throw new IllegalArgumentException("Unknown writePath Provided for writing the DataFrame");
    }
  }

  /**
   * Method to evaluate the write type from option
   *
   * @param writeType indicates if the write type is direct or indirect
   * @return if the write request is a direct write request or not
   */
  public boolean isDirectWrite(String writeType) {
    WriteMethod path = WriteMethod.getWriteMethod(writeType);
    if (path.equals(WriteMethod.DIRECT)) {
      this.isDirectWrite = true;
      return true;
    } else if (path.equals(WriteMethod.INDIRECT)) {
      this.isDirectWrite = false;
      return false;
    } else {
      throw new IllegalArgumentException("Unknown writePath Provided for writing the DataFrame");
    }
  }

  public boolean isDirectWrite() {
    return isDirectWrite;
  }

  public boolean isTableExists() {
    return tableExists;
  }
}
