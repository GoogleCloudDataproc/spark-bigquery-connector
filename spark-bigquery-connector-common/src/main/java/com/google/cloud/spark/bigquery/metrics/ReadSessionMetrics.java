package com.google.cloud.spark.bigquery.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import java.util.HashMap;
import java.util.Map;

public class ReadSessionMetrics {
  private static final String bytesRead = "bqBytesRead";
  private static final String rowsRead = "bqRowsRead";
  private static final String scanTime = "bqScanTime";
  private static final String parseTime = "bqParseTime";

  public static class SessionMetrics {
    private final Counter bytesReadCounter = new Counter();
    private final Counter rowsReadCounter = new Counter();
    private final Counter scanTimeCounter = new Counter();
    private final Counter parseTimeCounter = new Counter();

    public Map<String, Metric> getNameToMetricMap() {
      Map<String, Metric> nameToMetricMap = new HashMap<>();
      nameToMetricMap.put(bytesRead, bytesReadCounter);
      nameToMetricMap.put(rowsRead, rowsReadCounter);
      nameToMetricMap.put(scanTime, scanTimeCounter);
      nameToMetricMap.put(parseTime, parseTimeCounter);
      return nameToMetricMap;
    }

    public Counter getBytesReadCounter() {
      return bytesReadCounter;
    }

    public Counter getRowsReadCounter() {
      return rowsReadCounter;
    }

    public Counter getScanTimeCounter() {
      return scanTimeCounter;
    }

    public Counter getParseTimeCounter() {
      return parseTimeCounter;
    }
  }

  private static Map<String, SessionMetrics> sessionMetricsCache;

  public static SessionMetrics forSession(String session) {
    sessionMetricsCache.putIfAbsent(session, new SessionMetrics());
    return sessionMetricsCache.get(session);
  }

  public static void createReadSessionMetricsMap() {
    sessionMetricsCache = new HashMap<>();
  }

  public static Map<String, SessionMetrics> getReadSessionMetricsMap() {
    return sessionMetricsCache;
  }

  public static void clearReadSessionMetricsMap() {
    sessionMetricsCache.clear();
  }
}
