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
package com.google.cloud.spark.bigquery.metrics;

import java.lang.reflect.Method;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkBigQueryConnectorMetricsUtils {
  private static final Logger logger =
      LoggerFactory.getLogger(SparkBigQueryConnectorMetricsUtils.class);

  public static void postInputFormatEvent(SparkContext sparkContext) {
    try {
      Class<?> eventClass = Class.forName("com.google.cloud.spark.events.InputFormatEvent");

      sparkContext
          .listenerBus()
          .post(
              (SparkListenerEvent)
                  eventClass.getConstructor(String.class, long.class).newInstance("bigquery", -1L));
    } catch (ReflectiveOperationException ignored) {
      logger.info("spark.events.InputFormatEvent library not in class path");
    }
  }

  public static void postConnectorVersion(SparkContext sparkContext, String connectorVersion) {

    try {
      Class<?> eventBuilderClass =
          Class.forName(
              "com.google.cloud.spark.events.BigQueryConnectorVersionEvent$BigQueryConnectorVersionEventBuilder");
      Method buildMethod = eventBuilderClass.getDeclaredMethod("build");

      sparkContext
          .listenerBus()
          .post(
              (SparkListenerEvent)
                  buildMethod.invoke(
                      eventBuilderClass
                          .getDeclaredConstructor(String.class)
                          .newInstance(connectorVersion)));
    } catch (ReflectiveOperationException ignored) {
      logger.info("spark.events.BigQueryConnectorVersionEvent library not in class path");
    }
  }

  /*
   * The session name is of the form projects/<project_id>/locations/<location>/sessions/<session_id>.
   * This method extracts the session id from the session name to get the shortest possible unique identifier for a session.
   */
  public static String extractDecodedSessionIdFromSessionName(String sessionName) {
    return sessionName.split("/")[5];
  }

  public static String getAccumulatorNameForMetric(String metricName, String sessionName) {
    return String.format("%s-%s", extractDecodedSessionIdFromSessionName(sessionName), metricName);
  }
}
