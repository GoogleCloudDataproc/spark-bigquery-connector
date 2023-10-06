package com.google.cloud.spark.bigquery.plugins;

import static com.google.cloud.spark.bigquery.plugins.SparkBigQueryPluginUtil.FAILED_MESSAGE;
import static com.google.cloud.spark.bigquery.plugins.SparkBigQueryPluginUtil.SUCCESS_MESSAGE;

import com.google.cloud.spark.events.BigQueryConnectorMetricEvent;
import com.google.cloud.spark.events.MetricJson;
import com.google.gson.JsonSyntaxException;
import java.util.Collections;
import java.util.Map;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkBigQueryDriverPlugin implements DriverPlugin {
  private static final Logger logger = LoggerFactory.getLogger(SparkBigQueryDriverPlugin.class);
  private SparkContext sparkContext;

  @Override
  public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
    this.sparkContext = sc;
    return Collections.emptyMap();
  }

  @Override
  public Object receive(Object message) throws Exception {
    try {
      if (message instanceof MetricJson) {
        MetricJson metricJson = (MetricJson) message;
        sparkContext.listenerBus().post(new BigQueryConnectorMetricEvent(metricJson));
      }
      return SUCCESS_MESSAGE;
    } catch (JsonSyntaxException j) {
      logger.warn("Unable to post to spark listener bus");
      j.printStackTrace();
    }
    return FAILED_MESSAGE;
  }
}
