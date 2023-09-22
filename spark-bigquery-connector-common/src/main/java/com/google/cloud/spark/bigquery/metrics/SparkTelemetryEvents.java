package com.google.cloud.spark.bigquery.metrics;

import com.google.cloud.spark.events.BigQueryConnectorVersionEvent;
import com.google.cloud.spark.events.InputFormatEvent;
import java.io.Serializable;
import org.apache.spark.sql.SQLContext;

public class SparkTelemetryEvents implements Serializable {
  private final SQLContext sqlContext;

  public SparkTelemetryEvents(SQLContext sparkContext) {
    this.sqlContext = sparkContext;
  }

  public void updateInputFormatEvent() {
    sqlContext.sparkContext().listenerBus().post(new InputFormatEvent("bigquery", -1));
  }

  public void updateConnectorVersion(String connectorVersion) {
    sqlContext
        .sparkContext()
        .listenerBus()
        .post(new BigQueryConnectorVersionEvent(connectorVersion));
  }
}
