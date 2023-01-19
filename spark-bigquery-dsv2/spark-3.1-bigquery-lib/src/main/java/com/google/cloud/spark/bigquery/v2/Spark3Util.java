/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.inject.Injector;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.spark.sql.types.StructType;

public class Spark3Util {

  static <T> T createBigQueryTableInstance(
      Class<T> clazz, Injector injector, TableId tableId, StructType sparkProvidedSchema) {
    try {
      Constructor<T> constructor =
          clazz.getConstructor(Injector.class, TableId.class, StructType.class);
      BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
      SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
      TableInfo tableInfo = bigQueryClient.getReadTable(config.toReadTableOptions());
      if (tableInfo == null) {
        return constructor.newInstance(injector, tableId, sparkProvidedSchema);
      }
      StructType schema =
          sparkProvidedSchema != null
              ? sparkProvidedSchema
              : SchemaConverters.toSpark(tableInfo.getDefinition().getSchema());
      return constructor.newInstance(injector, tableInfo.getTableId(), schema);
    } catch (NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new BigQueryConnectorException("Could not create BigQueryTable instance", e);
    }
  }
}
