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
import java.util.HashMap;
import java.util.Map;

public abstract class TestSparkAppBase {

  /**
   * Executes the core Spark test logic. Creates its own SparkSession.
   *
   * @param testDataset The BigQuery dataset to use for the test.
   * @param testTable The BigQuery table to use for the test.
   * @param parameters Map of parsed CLI key-value parameters.
   * @return JsonObject containing execution status and results payload.
   * @throws Exception on any test failure states.
   */
  public abstract JsonObject run(
      String testDataset, String testTable, Map<String, String> parameters) throws Exception;

  /**
   * Helper to execute the application on a Dataproc cluster.
   *
   * @param args CLI arguments passed to the Spark application main entry point.
   * @param app The concrete test application instance to run.
   */
  public static void runApp(String[] args, TestSparkAppBase app) {
    try {
      SparkAppArgs parsedArgs = parseArgs(args);

      JsonObject result = app.run(parsedArgs.dataset, parsedArgs.table, parsedArgs.parameters);

      System.out.println("===BEGIN OUTPUT===");
      System.out.println(result.toString());
      System.out.println("===END OUTPUT===");
    } catch (Throwable t) {
      System.err.println(
          "Execution error in Spark App " + app.getClass().getName() + ": " + t.getMessage());
      t.printStackTrace();

      JsonObject errorResult = new JsonObject();
      errorResult.addProperty("status", "error");
      errorResult.addProperty("errorMessage", t.getMessage());

      System.out.println("===BEGIN OUTPUT===");
      System.out.println(errorResult.toString());
      System.out.println("===END OUTPUT===");
      System.exit(1);
    }
  }

  private static SparkAppArgs parseArgs(String[] args) {
    String dataset = null;
    String table = null;
    Map<String, String> parameters = new HashMap<>();

    for (int i = 0; i < args.length; i++) {
      if ("--dataset".equals(args[i]) && i + 1 < args.length) {
        dataset = args[++i];
      } else if ("--table".equals(args[i]) && i + 1 < args.length) {
        table = args[++i];
      } else if (args[i].startsWith("--") && i + 1 < args.length) {
        String key = args[i].substring(2); // strip leading --
        String value = args[++i];
        parameters.put(key, value);
      }
    }

    if (dataset == null || table == null) {
      throw new IllegalArgumentException(
          "Missing required arguments: --dataset and --table must be specified.");
    }

    return new SparkAppArgs(dataset, table, parameters);
  }

  private static class SparkAppArgs {
    final String dataset;
    final String table;
    final Map<String, String> parameters;

    SparkAppArgs(String dataset, String table, Map<String, String> parameters) {
      this.dataset = dataset;
      this.table = table;
      this.parameters = parameters;
    }
  }
}
