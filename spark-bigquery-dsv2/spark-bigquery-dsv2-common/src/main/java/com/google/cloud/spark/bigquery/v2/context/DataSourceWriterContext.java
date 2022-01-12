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
package com.google.cloud.spark.bigquery.v2.context;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.v2.InjectorFactory;
import com.google.inject.Injector;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Optional;

public interface DataSourceWriterContext {

  DataWriterContextFactory<InternalRow> createWriterContextFactory();

  default boolean useCommitCoordinator() {
    return true;
  }

  default void onDataWriterCommit(WriterCommitMessageContext message) {}

  void commit(WriterCommitMessageContext[] messages);

  void abort(WriterCommitMessageContext[] messages);

  static  Optional<DataSourceWriterContext> create(
          String writeUUID, StructType schema, SaveMode mode, Map<String, String> options) {
    Injector injector =
            InjectorFactory.createInjector(
                    schema, options, new BigQueryDataSourceWriterModule(writeUUID, schema, mode));
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
      boolean createNever =
              config
                      .getCreateDisposition()
                      .map(createDisposition -> createDisposition == JobInfo.CreateDisposition.CREATE_NEVER)
                      .orElse(false);
      if (createNever) {
        throw new IllegalArgumentException(
                String.format(
                        "For table %s Create Disposition is CREATE_NEVER and the table does not exists."
                                + " Aborting the insert",
                        BigQueryUtil.friendlyTableName(config.getTableId())));
      }
    }
    DataSourceWriterContext dataSourceWriterContext = null;
    switch (config.getWriteMethod()) {
      case DIRECT:
        dataSourceWriterContext = injector.getInstance(BigQueryDirectDataSourceWriterContext.class);
        break;
      case INDIRECT:
        dataSourceWriterContext =
                injector.getInstance(BigQueryIndirectDataSourceWriterContext.class);
        break;
    }
    return Optional.of(dataSourceWriterContext);
  }

}
