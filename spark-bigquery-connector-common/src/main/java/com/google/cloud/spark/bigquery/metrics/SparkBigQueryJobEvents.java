package com.google.cloud.spark.bigquery.metrics;

import com.google.cloud.spark.events.BigQueryConnectorReadStreamEvent;
import com.google.cloud.spark.events.BigQueryConnectorVersionEvent;
import com.google.cloud.spark.events.InputFormatEvent;
import org.apache.spark.sql.SQLContext;

public class SparkBigQueryJobEvents {

  public static void postInputFormatEvent(SQLContext sqlContext) {
    sqlContext.sparkContext().listenerBus().post(new InputFormatEvent("bigquery", -1));
  }

  public static void postConnectorVersion(SQLContext sqlContext, String connectorVersion) {
    sqlContext
        .sparkContext()
        .listenerBus()
        .post(new BigQueryConnectorVersionEvent(connectorVersion));
  }

  public static void postReadStreamsPerSession(
      SQLContext sqlContext, String sessionID, int streamCount) {
    sqlContext
        .sparkContext()
        .listenerBus()
        .post(new BigQueryConnectorReadStreamEvent(sessionID, streamCount));
  }

  public static String extractDecodedSessionIDFromSessionName(String streamName) {
    return streamName.split("/")[5];
  }

  public static String extractDecodedSessionIDFromStreamName(String streamName) {
    return extractDecodedSessionIDFromSessionName(streamName);
  }
}
