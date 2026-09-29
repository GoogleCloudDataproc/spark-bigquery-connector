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
package com.google.cloud.spark.bigquery.integration;

import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class SparkBigQueryJUnitRunner {
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: SparkBigQueryJUnitRunner <TestClass> [<TestMethod>]");
      System.exit(1);
    }

    String className = args[0];
    Class<?> testClass = Class.forName(className);

    Result result;
    if (args.length > 1) {
      String methodName = args[1];
      Request request = Request.method(testClass, methodName);
      result = new JUnitCore().run(request);
    } else {
      result = JUnitCore.runClasses(testClass);
    }

    for (Failure failure : result.getFailures()) {
      System.err.println(failure.toString());
      if (failure.getException() != null) {
        failure.getException().printStackTrace();
      }
    }

    if (!result.wasSuccessful()) {
      System.exit(1);
    }
  }
}
