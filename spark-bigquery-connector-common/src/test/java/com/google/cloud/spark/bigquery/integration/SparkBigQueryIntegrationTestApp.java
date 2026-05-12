/*
 * Copyright 2026 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.integration;

import com.google.gson.JsonObject;
import java.util.Map;

@FunctionalInterface
public interface SparkBigQueryIntegrationTestApp {

  /**
   * Executes the core Spark test logic. Creates its own SparkSession.
   *
   * @param testDataset The BigQuery dataset to use for the test.
   * @param testTable The BigQuery table to use for the test.
   * @param parameters Map of parsed CLI key-value parameters.
   * @return JsonObject containing execution status and results payload.
   * @throws Exception on any test failure states.
   */
  JsonObject run(String testDataset, String testTable, Map<String, String> parameters)
      throws Exception;
}
