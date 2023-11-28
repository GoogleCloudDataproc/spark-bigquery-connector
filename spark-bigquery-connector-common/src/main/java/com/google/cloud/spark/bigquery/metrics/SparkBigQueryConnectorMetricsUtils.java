package com.google.cloud.spark.bigquery.metrics;

import java.lang.reflect.Method;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;

public class SparkBigQueryConnectorMetricsUtils {
  public static void postInputFormatEvent(SparkContext sparkContext) {
    try {
      Class<?> eventClass = Class.forName("com.google.cloud.spark.events.InputFormatEvent");

      sparkContext
          .listenerBus()
          .post(
              (SparkListenerEvent)
                  eventClass.getConstructor(String.class, Long.class).newInstance("bigquery", -1L));
    } catch (ReflectiveOperationException ignored) {
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
      System.out.println(ignored);
    }
  }

  public static String extractDecodedSessionIdFromSessionName(String sessionName) {
    return sessionName;
  }

  public static String getAccumulatorNameForMetric(String metricName, String sessionName) {
    return String.format("%s-%s", extractDecodedSessionIdFromSessionName(sessionName), metricName);
  }
}
