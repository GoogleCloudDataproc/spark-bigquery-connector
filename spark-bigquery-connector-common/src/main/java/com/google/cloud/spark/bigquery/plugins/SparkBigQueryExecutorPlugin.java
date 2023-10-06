package com.google.cloud.spark.bigquery.plugins;

import static com.google.cloud.spark.bigquery.plugins.SparkBigQueryPluginUtil.FAILED_MESSAGE;
import static com.google.cloud.spark.bigquery.plugins.SparkBigQueryPluginUtil.SUCCESS_MESSAGE;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.google.cloud.spark.bigquery.metrics.ReadSessionMetrics;
import com.google.cloud.spark.events.MetricJson;
import java.util.Map;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;

public class SparkBigQueryExecutorPlugin implements ExecutorPlugin {
  private PluginContext ctx;

  @Override
  public void init(PluginContext ctx, Map<String, String> extraConf) {
    this.ctx = ctx;
    ReadSessionMetrics.createReadSessionMetricsMap();
  }

  private void sendMessageToDriver(String sessionID, String metricName, Metric metric)
      throws Exception {
    Counter metricCounter = (Counter) metric;
    if (metricCounter.getCount() == 0) {
      //if the metric doesn't have any value, we don't need to send it.
      return;
    }
    MetricJson metricJson =
        new MetricJson(ctx.executorID(), sessionID, metricName, metricCounter.getCount());
    Object status = ctx.ask(metricJson);
    if (status.toString().equals(SUCCESS_MESSAGE)) {
      metricCounter.dec(metricCounter.getCount());
    } else {
      throw new RuntimeException("Unable to send the message to driver");
    }
  }

  @Override
  public void onTaskSucceeded() {
    Map<String, ReadSessionMetrics.SessionMetrics> map =
        ReadSessionMetrics.getReadSessionMetricsMap();
    for (Map.Entry<String, ReadSessionMetrics.SessionMetrics> e : map.entrySet()) {
      try {
        String sessionID = e.getKey();
        ReadSessionMetrics.SessionMetrics tableMetrics = e.getValue();
        for (Map.Entry<String, Metric> me : tableMetrics.getNameToMetricMap().entrySet()) {
          sendMessageToDriver(sessionID, me.getKey(), me.getValue());
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Override
  public void shutdown() {
    ReadSessionMetrics.clearReadSessionMetricsMap();
  }
}
