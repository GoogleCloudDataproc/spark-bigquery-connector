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

import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.lang.reflect.Method;
import java.util.Optional;
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
      logger.debug("spark.events.InputFormatEvent library not in class path");
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
      logger.debug("spark.events.BigQueryConnectorVersionEvent library not in class path");
    }
  }

  public static void postWriteSessionMetrics(
      long timestamp,
      SparkBigQueryConfig.WriteMethod writeMethod,
      long bytesWritten,
      Optional<SparkBigQueryConfig.IntermediateFormat> intermediateDataFormat,
      SparkContext sparkContext) {
    try {
      // Reflection is used here to load classes in spark events jar which is google
      // internal and not available during complie time.
      Class<?> eventBuilderClass =
          Class.forName(
              "com.google.cloud.spark.events.SparkBigQueryConnectorWriteEvent$SparkBigQueryConnectorWriteEventBuilder");

      Object builderInstance =
          eventBuilderClass.getDeclaredConstructor(long.class).newInstance(timestamp);
      eventBuilderClass
          .getMethod("setBytesWritten", long.class)
          .invoke(builderInstance, bytesWritten);

      if (intermediateDataFormat.isPresent()) {
        Class<?> dataFormatEnum = Class.forName("com.google.cloud.spark.events.DataFormat");
        Object[] dataFormatEnumConstants = dataFormatEnum.getEnumConstants();
        Object generatedDataFormatEnumValue = dataFormatEnumConstants[0];
        for (Object constant : dataFormatEnumConstants) {
          Method nameMethod = constant.getClass().getMethod("name");
          String name = (String) nameMethod.invoke(constant);
          if (name.equalsIgnoreCase(intermediateDataFormat.get().getDataSource())) {
            generatedDataFormatEnumValue = constant;
            break;
          }
        }
        eventBuilderClass
            .getMethod("setIntermediateDataFormat", dataFormatEnum)
            .invoke(builderInstance, generatedDataFormatEnumValue);
      }

      Class<?> dataWriteMethodEnum = Class.forName("com.google.cloud.spark.events.DataWriteMethod");
      Object[] dataWriteMethodConstants = dataWriteMethodEnum.getEnumConstants();
      Object generatedDataWriteMethodEnumValue = dataWriteMethodConstants[0];
      for (Object constant : dataWriteMethodConstants) {
        Method nameMethod = constant.getClass().getMethod("name");
        String name = (String) nameMethod.invoke(constant);
        if (name.equalsIgnoreCase(writeMethod.toString())) {
          generatedDataWriteMethodEnumValue = constant;
          break;
        }
      }
      eventBuilderClass
          .getMethod("setWriteMethod", dataWriteMethodEnum)
          .invoke(builderInstance, generatedDataWriteMethodEnumValue);

      Method buildMethod = eventBuilderClass.getDeclaredMethod("build");

      sparkContext.listenerBus().post((SparkListenerEvent) buildMethod.invoke(builderInstance));

    } catch (ReflectiveOperationException ex) {
      logger.debug("spark.events.SparkBigQueryConnectorWriteEvent library not in class path");
    }
  }

  public static String getAccumulatorNameForMetric(String metricName, String sessionName) {
    return String.format("%s-%s", sessionName, metricName);
  }
}
