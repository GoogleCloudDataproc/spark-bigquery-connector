/*
 * Copyright 2025 Google Inc. All Rights Reserved.
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

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.immutable.Map;

/**
 * This class is created only for compatability with OpenLineage, since it expects an instance of
 * this class <a *
 * href="https://github.com/OpenLineage/OpenLineage/blob/9ec3f262901f812b8f43c19262770d2fcad5ae9a/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/BigQueryNodeOutputVisitor.java#L55">here</a>.
 * Additional usage of this class for V2 connector is discouraged.
 */
public abstract class BigQueryRelationProvider {
  public SparkBigQueryConfig createSparkBigQueryConfig(
      SQLContext sqlContext, Map<String, String> options, Option<StructType> schema) {
    return SparkBigQueryUtil.createSparkBigQueryConfig(
        sqlContext, options, schema, DataSourceVersion.V2);
  }
}
