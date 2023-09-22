package com.google.cloud.spark.bigquery.plugins;

import static com.google.cloud.spark.bigquery.plugins.SparkBigQueryPluginUtil.FAILED_MESSAGE;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import java.util.Map;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;

public class SparkBigQueryExecutorPlugin implements ExecutorPlugin {
  private PluginContext ctx;

  @Override
  public void init(PluginContext ctx, Map<String, String> extraConf) {
    this.ctx = ctx;
    ctx.metricRegistry()
        .register(SparkBigQueryPluginUtil.scanTime, SparkBigQueryPluginUtil.scanTimeCounter);
    ctx.metricRegistry()
        .register(SparkBigQueryPluginUtil.parseTime, SparkBigQueryPluginUtil.parseTimeCounter);
    ctx.metricRegistry()
        .register(SparkBigQueryPluginUtil.bytesRead, SparkBigQueryPluginUtil.bytesReadCounter);
    ctx.metricRegistry()
        .register(SparkBigQueryPluginUtil.rowsRead, SparkBigQueryPluginUtil.rowsReadCounter);
  }

  @Override
  public void onTaskSucceeded() {
    try {
      Map<String, Metric> metrics = ctx.metricRegistry().getMetrics();
      for (Map.Entry<String, Metric> e : metrics.entrySet()) {
        if (e.getKey().equals(SparkBigQueryPluginUtil.scanTime)
            || e.getKey().equals(SparkBigQueryPluginUtil.parseTime)
            || e.getKey().equals(SparkBigQueryPluginUtil.bytesRead)
            || e.getKey().equals(SparkBigQueryPluginUtil.rowsRead)) {
          MetricJson metricJson =
              new MetricJson(ctx.executorID(), e.getKey(), ((Counter) e.getValue()).getCount());
          Object status = ctx.ask(metricJson);
          if (status.toString().equals(FAILED_MESSAGE)) {
            throw new RuntimeException("");
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
