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
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.BigQueryConfig;
import com.google.cloud.bigquery.connector.common.EnvironmentContext;
import com.google.cloud.bigquery.connector.common.UserAgentProvider;
import com.google.cloud.spark.bigquery.metrics.SparkTelemetryEvents;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class SparkBigQueryConnectorModule implements Module {

  private final SparkSession spark;
  private final Map<String, String> options;
  private final Map<String, String> customDefaults;
  private final Optional<StructType> schema;
  private final DataSourceVersion dataSourceVersion;
  private final boolean tableIsMandatory;
  private final Optional<SparkBigQueryConfig> config;

  public SparkBigQueryConnectorModule(
      SparkSession spark,
      Map<String, String> options,
      Map<String, String> customDefaults,
      Optional<StructType> schema,
      DataSourceVersion dataSourceVersion,
      boolean tableIsMandatory,
      Optional<SparkBigQueryConfig> config) {
    this.spark = spark;
    this.options = options;
    this.customDefaults = customDefaults;
    this.schema = schema;
    this.dataSourceVersion = dataSourceVersion;
    this.tableIsMandatory = tableIsMandatory;
    this.config = config;
  }

  @Override
  public void configure(Binder binder) {
    binder.bind(BigQueryConfig.class).toProvider(this::provideSparkBigQueryConfig);
  }

  @Singleton
  @Provides
  public SparkSession provideSparkSession() {
    return spark;
  }

  @Singleton
  @Provides
  public DataSourceVersion provideDataSourceVersion() {
    return dataSourceVersion;
  }

  @Singleton
  @Provides
  public SparkBigQueryConfig provideSparkBigQueryConfig() {
    return config.orElseGet(
        () ->
            SparkBigQueryConfig.from(
                options,
                ImmutableMap.copyOf(customDefaults),
                dataSourceVersion,
                spark,
                schema,
                tableIsMandatory));
  }

  @Singleton
  @Provides
  public UserAgentProvider provideUserAgentProvider() {
    return new SparkBigQueryConnectorUserAgentProvider(dataSourceVersion.toString().toLowerCase());
  }

  @Singleton
  @Provides
  public EnvironmentContext provideEnvironmentContext() {
    return new EnvironmentContext(
        SparkBigQueryUtil.extractJobLabels(spark.sparkContext().getConf()));
  }
}
