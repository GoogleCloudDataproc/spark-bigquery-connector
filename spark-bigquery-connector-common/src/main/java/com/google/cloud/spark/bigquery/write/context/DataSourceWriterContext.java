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
package com.google.cloud.spark.bigquery.write.context;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.inject.Injector;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

public interface DataSourceWriterContext {

  DataWriterContextFactory<InternalRow> createWriterContextFactory();

  default boolean useCommitCoordinator() {
    return true;
  }

  default void onDataWriterCommit(WriterCommitMessageContext message) {}

  void commit(WriterCommitMessageContext[] messages);

  void abort(WriterCommitMessageContext[] messages);

  static Optional<DataSourceWriterContext> create(
      Injector injector,
      String writeUUID,
      StructType schema,
      SaveMode mode,
      Map<String, String> options) {
    SparkBigQueryConfig tableConfig = injector.getInstance(SparkBigQueryConfig.class);
    if (tableConfig.getTableId() == null) {
      // the config was created for the catalog, we need to parse the tableId from the options
      SparkSession spark = injector.getInstance(SparkSession.class);
      tableConfig =
          SparkBigQueryConfig.from(
              options,
              DataSourceVersion.V2,
              spark,
              Optional.of(schema),
              true /* tableIsMandatory */);
    }
    Injector writerInjector =
        injector.createChildInjector(
            new BigQueryDataSourceWriterModule(tableConfig, writeUUID, schema, mode));
    // first verify if we need to do anything at all, based on the table existence and the save
    // mode.
    BigQueryClient bigQueryClient = writerInjector.getInstance(BigQueryClient.class);
    TableId tableId = tableConfig.getTableId();
    TableInfo table = bigQueryClient.getTable(tableId);
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
      boolean createNever =
          tableConfig
              .getCreateDisposition()
              .map(createDisposition -> createDisposition == JobInfo.CreateDisposition.CREATE_NEVER)
              .orElse(false);
      if (createNever) {
        throw new IllegalArgumentException(
            String.format(
                "For table %s Create Disposition is CREATE_NEVER and the table does not exists."
                    + " Aborting the insert",
                BigQueryUtil.friendlyTableName(tableId)));
      }
    }
    DataSourceWriterContext dataSourceWriterContext = null;
    switch (tableConfig.getWriteMethod()) {
      case DIRECT:
        dataSourceWriterContext =
            writerInjector.getInstance(BigQueryDirectDataSourceWriterContext.class);
        break;
      case INDIRECT:
        dataSourceWriterContext =
            writerInjector.getInstance(BigQueryIndirectDataSourceWriterContext.class);
        break;
    }
    return Optional.of(dataSourceWriterContext);
  }
}
