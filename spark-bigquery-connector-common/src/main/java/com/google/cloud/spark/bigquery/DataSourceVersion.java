/*
 * Copyright 2022 Google LLC
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

import java.util.Map;

/**
 * A single place to put data source implementations customizations, in order to avoid a general
 * `if(dataSource==v1) {...} else {...}` throughout the code.
 */
public enum DataSourceVersion {
  V1,
  V2;

  public void updateOptionsMap(Map<String, String> optionsMap) {
    // these updates are v2 only
    if (this == V2) {
      // no need for the spark-avro module, we have an internal copy of avro
      optionsMap.put(SparkBigQueryConfig.VALIDATE_SPARK_AVRO_PARAM.toLowerCase(), "false");
      // DataSource V2 implementation uses Java only
      optionsMap.put(
          SparkBigQueryConfig.INTERMEDIATE_FORMAT_OPTION.toLowerCase(),
          SparkBigQueryConfig.IntermediateFormat.AVRO.toString());
    }
  }
}
