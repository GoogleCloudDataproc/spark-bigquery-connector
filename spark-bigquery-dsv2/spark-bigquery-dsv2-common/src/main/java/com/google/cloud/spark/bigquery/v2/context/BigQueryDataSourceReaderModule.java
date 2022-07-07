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

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;

public class BigQueryDataSourceReaderModule implements Module {

  private Optional<SparkBigQueryConfig> tableScanConfig;

  public BigQueryDataSourceReaderModule() {
    this(Optional.empty());
  }

  // in practiced used only by the spark 3 connector, as there are separate phases for creating the
  // catalog and the table scan (unlike DSv1 and spark 2.4)
  public BigQueryDataSourceReaderModule(Optional<SparkBigQueryConfig> tableScanConfig) {
    this.tableScanConfig = tableScanConfig;
  }

  @Override
  public void configure(Binder binder) {
    // empty
  }

  @Singleton
  @Provides
  public BigQueryDataSourceReaderContext provideDataSourceReaderContext(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      SparkBigQueryConfig globalConfig,
      SparkSession sparkSession) {
    SparkBigQueryConfig config = tableScanConfig.orElse(globalConfig);
    TableInfo tableInfo = bigQueryClient.getReadTable(config.toReadTableOptions());
    return new BigQueryDataSourceReaderContext(
        tableInfo,
        bigQueryClient,
        bigQueryReadClientFactory,
        tracerFactory,
        config.toReadSessionCreatorConfig(),
        config.getFilter(),
        config.getSchema(),
        sparkSession.sparkContext().applicationId(),
        config,
        sparkSession.sqlContext());
  }
}
