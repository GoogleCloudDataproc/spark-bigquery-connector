package com.google.cloud.spark.bigquery.v2.customMetrics;

import static org.apache.spark.util.Utils.bytesToString;
import static org.apache.spark.util.Utils.msDurationToString;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Locale;

class MetricUtils {

  private static final String METRIC_FORMAT = "total (min, med, max)\n%s (%s, %s, %s)";
  private static final NumberFormat NUMBER_FORMAT_US = NumberFormat.getIntegerInstance(Locale.US);

  static String formatTimeMetrics(long[] taskMetrics) {
    if (taskMetrics.length == 0) {
      return msDurationToString(0);
    }
    if (taskMetrics.length == 1) {
      return msDurationToString(taskMetrics[0]);
    }
    Arrays.sort(taskMetrics);
    String sum = msDurationToString(Arrays.stream(taskMetrics).sum());
    String min = msDurationToString(taskMetrics[0]);
    String med = msDurationToString(taskMetrics[taskMetrics.length / 2]);
    String max = msDurationToString(taskMetrics[taskMetrics.length - 1]);
    return String.format(METRIC_FORMAT, sum, min, med, max);
  }

  static String formatSizeMetrics(long[] taskMetrics) {
    return bytesToString(Arrays.stream(taskMetrics).sum());
  }

  static String formatSumMetrics(long[] taskMetrics) {
    return NUMBER_FORMAT_US.format(Arrays.stream(taskMetrics).sum());
  }
}
