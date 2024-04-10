/*
 * Copyright 2024 Google LLC
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

package com.google.cloud.spark.bigquery;

import static scala.collection.JavaConverters.mapAsJavaMap;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Optional;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.immutable.Map;

/** For OpenLineage */
public abstract class BigQueryRelationProvider {
  public SparkBigQueryConfig createSparkBigQueryConfig(
      SQLContext sqlContext, Map<String, String> options, Option<StructType> schema) {
    java.util.Map<String, String> optionsMap = new HashMap<>(mapAsJavaMap(options));
    DataSourceVersion.V1.updateOptionsMap(optionsMap);
    SparkSession spark = sqlContext.sparkSession();
    ImmutableMap<String, String> globalOptions =
        ImmutableMap.copyOf(mapAsJavaMap(spark.conf().getAll()));

    return SparkBigQueryConfig.from(
        ImmutableMap.copyOf(optionsMap),
        globalOptions,
        spark.sparkContext().hadoopConfiguration(),
        ImmutableMap.of(),
        1,
        spark.sqlContext().conf(),
        spark.version(),
        Optional.ofNullable(schema.getOrElse(null)),
        true);
  }
}
