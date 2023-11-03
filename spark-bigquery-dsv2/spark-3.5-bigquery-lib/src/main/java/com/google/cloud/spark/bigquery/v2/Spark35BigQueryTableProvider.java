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

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

public class Spark35BigQueryTableProvider extends Spark34BigQueryTableProvider {
  // empty
  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return Spark3Util.createBigQueryTableInstance(Spark33BigQueryTable::new, schema, properties);
  }

  @Override
  protected Table getBigQueryTableInternal(Map<String, String> properties) {
    return Spark3Util.createBigQueryTableInstance(Spark33BigQueryTable::new, null, properties);
  }
}
