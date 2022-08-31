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

public class InjectorFactory {
  private InjectorFactory() {}

  public static Injector createInjector(
      StructType schema, Map<String, String> options, boolean tableIsMandatory) {
    SparkSession spark = SparkSession.active();
    return createInjector(spark, schema, options, tableIsMandatory);
  }

  public static Injector createInjector(
      SparkSession spark,
      StructType schema,
      Map<String, String> options,
      boolean tableIsMandatory) {
    Map<String, String> customDefaults = ImmutableMap.of();
    return Guice.createInjector(
        new BigQueryClientModule(),
        new SparkBigQueryConnectorModule(
            spark,
            options,
            customDefaults,
            Optional.ofNullable(schema),
            DataSourceVersion.V2,
            tableIsMandatory,
            Optional.empty()));
  }
}
