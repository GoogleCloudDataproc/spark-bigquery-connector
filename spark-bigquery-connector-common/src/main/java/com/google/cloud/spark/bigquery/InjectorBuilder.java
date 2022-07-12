/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class InjectorBuilder {

  private SparkSession spark = SparkSession.active();
  private Optional<StructType> schema = Optional.empty();
  private Map<String, String> options = ImmutableMap.<String, String>of();
  private Map<String, String> customDefaults = ImmutableMap.<String, String>of();
  private boolean tableIsMandatory = true;
  private DataSourceVersion dataSourceVersion = DataSourceVersion.V2;

  public InjectorBuilder withSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }

  public InjectorBuilder withSchema(StructType schema) {
    this.schema = Optional.ofNullable(schema);
    return this;
  }

  public InjectorBuilder withOptions(Map<String, String> options) {
    this.options = ImmutableMap.copyOf(options);
    return this;
  }

  public InjectorBuilder withTableIsMandatory(boolean tableIsMandatory) {
    this.tableIsMandatory = tableIsMandatory;
    return this;
  }

  public InjectorBuilder withDataSourceVersion(DataSourceVersion dataSourceVersion) {
    this.dataSourceVersion = dataSourceVersion;
    return this;
  }

  public InjectorBuilder withCustomDefaults(Map<String, String> customDefaults) {
    this.customDefaults = customDefaults;
    return this;
  }

  public Injector build() {
    return Guice.createInjector(
        new BigQueryClientModule(),
        new SparkBigQueryConnectorModule(
            spark, options, customDefaults, schema, dataSourceVersion, tableIsMandatory));
  }
}
